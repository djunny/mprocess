<?php
/**
 * Date: 2020/1/2
 * Time: 11:31 下午
 */

namespace ZV;

use Swoole\Process;
use Swoole\Table;
use Swoole\Lock;

class MProcess implements \Countable, \ArrayAccess
{
    private $process      = 5;
    private $process_list = [];
    private $master_id    = 0;
    /**@var QueueTable */
    private $table      = NULL;
    private $table_size = 1024;
    private $data_size  = 1024;
    /**@var Table */
    private $atomic_table = [];
    /**@var Lock */
    private $locker = NULL;

    /**
     * MProcess constructor.
     *
     * @param int $process    process count
     * @param int $table_size queue table size
     * @param int $data_size  max-length for data(after json_encode)
     */
    public function __construct($process = 5, $table_size = -1, $data_size = 1024) {
        $this->process    = $process ?: 5;
        $this->master_id  = getmypid();
        $this->table_size = $table_size == -1 ? $process * 1024 : $table_size;
        $this->data_size  = $data_size;
        $this->init();
    }

    /**
     * destroy memory
     */
    public function __destruct() {
        $this->atomic_table->destroy();
        $this->locker->destroy();
    }

    /**
     * init table
     */
    private function init() {
        $this->table = new QueueTable($this->table_size, $this->data_size);
        // add atomic record
        $this->atomic_table = new Table($this->table_size);
        $this->atomic_table->column('id', Table::TYPE_INT, 4);
        $this->atomic_table->create();
        // lock
        $this->locker = new Lock();
    }

    /**
     * get table size
     *
     * @return mixed
     */
    function get_table_size() {
        return $this->table->get_size();
    }

    /**
     * get left process count
     *
     * @return int
     */
    function get_left_process() {
        return count($this->process_list);
    }

    /**
     *  task loop while all of done
     *
     * @param $callback
     *
     * @return $this
     */
    function loop($callback) {
        return $this->do(function ($index, $count) use ($callback) {
            $is_done = 0;
            while ($this->atomic('_loop') != $count) {
                $task_data = $this->shift();
                if ($task_data === NULL) {
                    if (!$is_done) {
                        $this->incr('_loop');
                        $is_done = 1;
                    }
                    // lower CPU time
                    usleep(100);
                    continue;
                }
                $callback($task_data, $index, $count);
            }
        });
    }

    /**
     * wait all process
     *
     * @return $this
     */
    function wait() {
        if (!$this->process_list) {
            return $this;
        }
        //监听到进程退出了
        while ($ret = Process::wait(TRUE)) {
            if (!$this->process_list) {
                break;
            }
            $pid = $ret['pid'] ?: FALSE;
            if ($pid && $this->process_list[$pid]) {
                // Close Event
                unset($this->process_list[$pid]);
            }
            usleep(1000);
        }
        return $this;
    }

    /**
     * do process
     *
     * @param $callback
     *
     * @return $this
     */
    function do($callback) {
        $this->process_list = [];
        for ($i = 0; $i < $this->process; $i++) {
            $process                  = $this->create_process($i, $this->process,
                function ($index, $count, $process) use ($callback) {
                    // auto set log prefix
                    if (class_exists('log')) {
                        \log::$prefix = $index . '/' . $count;
                    }
                    $callback($index, $count, $process);
                    $process->exit();
                });
            $pid                      = $process->start();
            $this->process_list[$pid] = $process;
        }
        return $this;
    }

    /**
     * lock to call
     *
     * @param $callback
     */
    public function lock($callback) {
        try {
            $this->locker->lock();
            //log::info('EnLock');
            $res = $callback();
        } finally {
            $this->locker->unlock();
            //log::info('UNLock');
        }
        return $res;
    }

    /**
     * create process
     *
     * @param $index
     * @param $count
     * @param $callback
     *
     * @return Process
     */
    private function create_process($index, $count, $callback) {
        $process = new Process(function ($process) use ($index, $count, $callback) {
            $callback($index, $count, $process);
        }, FALSE, FALSE);
        return $process;
    }


    /**
     * @inheritDoc
     */
    public function offsetExists($offset) {
        return isset($this->table[$offset]);
    }

    /**
     * @inheritDoc
     */
    public function offsetGet($offset) {
        return $this->table[$offset];
    }

    /**
     * @inheritDoc
     */
    public function offsetSet($offset, $value) {
        return $this->table->push($value, $offset);
    }

    /**
     * @inheritDoc
     */
    public function offsetUnset($offset) {
        unset($this->table[$offset]);
    }

    /**
     * @return mixed
     */
    function shift() {
        return $this->table->shift();
    }


    /**
     * @return mixed
     */
    function pop() {
        return $this->table->pop();
    }

    /**
     * push
     *
     * @param        $data
     * @param string $key
     *
     * @return mixed
     */
    function push($data, $key = '') {
        return $this->table->push($data, $key);
    }

    /**
     * encode key
     *
     * @param $key
     *
     * @return string
     */
    private function encode_key($key) {
        return ':' . $key;// 防止 key 是数字
    }

    /**
     * incr
     *
     * @param     $key
     * @param int $incrby
     *
     * @return bool
     */
    function incr($key, $incrby = 1) {
        return $this->atomic_table->incr($this->encode_key($key), 'id', $incrby);
    }

    /**
     * decr
     *
     * @param     $key
     * @param int $incrby
     */
    function decr($key, $incrby = 1) {
        return $this->atomic_table->decr($this->encode_key($key), 'id', $incrby);
    }

    /**
     * get atomic value
     *
     * @param $key
     *
     * @return array|bool|string
     */
    function atomic($key) {
        return $this->atomic_table->get($this->encode_key($key), 'id');
    }

    /**
     * @inheritDoc
     */
    public function count() {
        return count($this->table);
    }
}
