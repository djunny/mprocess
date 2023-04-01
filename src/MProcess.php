<?php
/**
 * Date: 2020/1/2
 * Time: 11:31 下午
 */

namespace ZV;

use Swoole\Process;
use Swoole\Table;
use Swoole\Lock;

class MProcess implements \Countable, \ArrayAccess {
    private $process      = 5;
    private $process_list = [];
    private $master_id;

    /**@var QueueTable */
    private $table;
    /**@var Table */
    private $atomic_table;

    private $table_size = 1024;
    private $data_size  = 1024;
    /**@var Lock */
    private $locker = NULL;
    /**
     * @var array event
     */
    private $events = [];

    // process timeout
    const EVENT_TIMEOUT = 'timeout';
    // process done
    const EVENT_DONE = 'done';
    // process init(start in first child process)
    const EVENT_INIT = 'init';

    const SLEEP_TIME = 1000;

    /**
     * MProcess constructor.
     *
     * @param int $process    process count
     * @param int $table_size queue table initial size
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
     * bind event
     *
     * @param $event
     * @param $callback
     *
     * @return $this
     */
    public function on($event, $callback) {
        $this->events[$event] = $callback;
        return $this;
    }

    /**
     * on timeout
     *
     * @param $callback
     *
     * @return $this
     */
    public function on_timeout($callback) {
        return $this->on(self::EVENT_TIMEOUT, $callback);
    }

    /**
     * on init process
     *
     * @param $callback
     *
     * @return $this
     */
    public function on_init($callback) {
        return $this->on(self::EVENT_INIT, $callback);
    }

    /**
     * on done
     *
     * @param $callback
     *
     * @return $this
     */
    public function on_done($callback) {
        return $this->on(self::EVENT_DONE, $callback);
    }

    /**
     * has event
     *
     * @param $event
     *
     * @return bool
     */
    public function has($event) {
        return isset($this->events[$event]);
    }

    /**
     * trigger event
     *
     * @param $event
     *
     * @return void
     */
    private function trigger($event, ...$args) {
        if (isset($this->events[$event])) {
            call_user_func_array($this->events[$event], $args);
        }
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
     * get left process
     *
     * @return array
     */
    function get_left_process() {
        if (!$this->process_list) {
            return [];
        }
        $ret = Process::wait(false);
        $pid = $ret['pid'] ?: FALSE;
        if ($pid && $this->process_list[$pid]) {
            // Close Event
            unset($this->process_list[$pid]);
        }
        return $this->process_list;
    }

    /**
     * wait all process
     *
     * @param int      $timout second wait
     * @param \Closure $timeout_callback
     *
     * @return $this
     */
    function wait($timeout = -1, $timeout_callback = NULL) {
        if (!$this->process_list) {
            return $this;
        }
        if ($timeout_callback) {
            $this->on(static::EVENT_TIMEOUT, $timeout_callback);
        }
        $start_time = time();
        while (true) {
            $ret = Process::wait($timeout == -1 ? true : false);
            if ($timeout > 0 && time() - $start_time > $timeout) {
                // kill all process
                /**@var Process $process */
                foreach ($this->process_list as $pid => $process) {
                    Process::kill($pid, SIGKILL);
                    $process->exit(SIGKILL);
                }
                $this->trigger(static::EVENT_TIMEOUT, $this);
                break;
            }
            if ($ret === false) {
                continue;
            }
            $pid = $ret['pid'] ?: FALSE;
            if ($pid && $this->process_list[$pid]) {
                // Close Event
                unset($this->process_list[$pid]);
            }
            if (!$this->process_list) {
                break;
            }
            usleep(static::SLEEP_TIME);
        }
        // done
        $this->trigger(static::EVENT_DONE, $this);
        return $this;
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
                    usleep(static::SLEEP_TIME);
                    continue;
                }
                $callback($task_data, $index, $count);
            }
        });
    }

    /**
     * lock to call
     *
     * @param $callback
     */
    public function lock($callback) {
        try {
            while (!$this->locker->trylock()) {
                usleep(static::SLEEP_TIME);
            }
            $res = $callback();
        } finally {
            $this->locker->unlock();
        }
        return $res;
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
        $that               = $this;
        for ($i = 0; $i < $this->process; $i++) {
            $process                  = $this->create_process($i, $this->process,
                function ($index, $count, $process) use ($callback, $that) {
                    // create init callback
                    if ($index == 0) {
                        $that->incr('_init');
                        try {
                            $this->trigger(static::EVENT_INIT, $that);
                        } catch (Exception $e) {
                            throw $e;
                        } finally {
                            $that->incr('_init');
                        }
                    } elseif ($that->has(static::EVENT_INIT)) {
                        // wait index=0 process init
                        while ($that->atomic('_init') !== 2) {
                            usleep(static::SLEEP_TIME);
                        }
                    }

                    // auto set log prefix
                    if (class_exists('log')) {
                        \log::$prefix = $index . '/' . $count;
                    }
                    $callback($index, $count, $that);
                    $process->exit();
                });
            $pid                      = $process->start();
            $this->process_list[$pid] = $process;
        }
        return $this;
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
        return $this->table[$offset] = $value;
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
        return ':' . $key;// keep string
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
        return (int)$this->atomic_table->incr($this->encode_key($key), 'id', $incrby);
    }

    /**
     * decr
     *
     * @param     $key
     * @param int $incrby
     */
    function decr($key, $incrby = 1) {
        return (int)$this->atomic_table->decr($this->encode_key($key), 'id', $incrby);
    }

    /**
     * get atomic value
     *
     * @param $key
     *
     * @return array|bool|string
     */
    function atomic($key) {
        return (int)$this->atomic_table->get($this->encode_key($key), 'id');
    }

    /**
     * remove atomic value
     *
     * @param $key
     *
     * @return array|bool|string
     */
    function atomic_del($key) {
        return (int)$this->atomic_table->del($this->encode_key($key));
    }

    /**
     * @inheritDoc
     */
    public function count() {
        return $this->table->count();
    }
}