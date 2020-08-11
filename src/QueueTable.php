<?php

/**
 * queue table
 * Date: 2020/8/1
 * Time: 下午5:28
 */

namespace ZV;

use Swoole\Table;
use Swoole\Atomic;
use Swoole\Lock;

class QueueTable extends Table implements \Countable, \ArrayAccess
{

    /**@var Table */
    private $meta_table;
    /**@var Atomic */
    private $max_atomic;
    /**@var Atomic */
    private $min_atomic;
    /**@vcar Lock */
    private $locker;

    public function __construct($size, $data_size) {
        parent::__construct($size);
        // id 是 meta 表自增的(4+4+464+size);
        //$this->column('id', Table::TYPE_INT, 4);
        $this->column('data', Table::TYPE_STRING, $data_size);
        $this->create();
        // meta table
        $this->meta_table = new Table($size * 2);
        $this->meta_table->column('id', Table::TYPE_INT, 8);
        //$this->meta_table->column('prev', Table::TYPE_INT, 4);
        $this->meta_table->column('key', Table::TYPE_STRING, 64);
        $this->meta_table->create();

        $this->max_atomic = new Atomic(0);
        $this->min_atomic = new Atomic(0);
        $this->locker     = new Lock(SWOOLE_MUTEX);
    }

    /**
     * destroy memory
     */
    public function __destruct() {
        $this->max_atomic = NULL;
        $this->min_atomic = NULL;
        $this->meta_table->destroy();
        $this->locker->destroy();
        $this->destroy();
    }


    public function get_size() {
        return $this->memorySize + $this->meta_table->memorySize;
    }

    private function incr_max_id() {
        $res = $this->max_atomic->add(1);
        return $res;
    }

    private function decr_max_id() {
        return $this->max_atomic->sub(1);
    }

    private function incr_min_id() {
        return $this->min_atomic->add(1);
    }

    private function get_max_id() {
        return $this->max_atomic->get();
    }

    private function get_min_id() {
        return $this->min_atomic->get();
    }


    private function encode_key($key) {
        return ':' . $key;// 防止 key 是数字
    }

    private function do_lock(\Closure $call) {
        try {
            $this->locker->lock();
            //log::info('EnLock');
            $res = $call();
        } finally {
            $this->locker->unlock();
            //log::info('UNLock');
        }
        return $res;
    }

    /**
     * do lock
     *
     * @param $callback
     *
     * @return mixed
     */
    public function lock($callback) {
        return $this->do_lock($callback);
    }

    public function push($data, $key = '') {
        $data = json_encode($data, 320);
        return $this->do_lock(function () use ($data, $key) {
            $max_id = $this->incr_max_id();
            //log::info('incr', $max_id);
            $key      = $key ?: $max_id;
            $data_key = $this->encode_key($max_id);
            if ($this->set($data_key, [
                'data' => $data,
            ])) {
                $this->meta_table->set($this->encode_key($key), [
                    'id' => $max_id,
                    //'prev' => $prev_id,
                ]);
                return TRUE;
            }
            return FALSE;
        });
    }


    function find($key) {
        return $this->do_lock(function () use ($key) {
            $id = $this->meta_table->get($this->encode_key($key), 'id');
            if ($id) {
                $data_key = $this->encode_key($id);
                $data     = $this->get($data_key, 'data');
                $data     = json_decode($data, 1);
                return $data;
            }
            return NULL;
        });
    }

    function shift() {
        // 如果 incr_min_id
        return $this->do_lock(function () {
            //log::info('sft', 'max=' . $this->get_max_id(), 'min=' . $this->get_min_id());
            $min_id = $this->get_min_id();
            while ($min_id <= $this->get_max_id()) {
                //log::info('min_id=' . $min_id);
                $data_key = $this->encode_key($min_id);
                if (!$this->exist($data_key)) {
                    $min_id = $this->incr_min_id();
                    continue;
                }
                $data = $this->get($data_key, 'data');
                $this->unset($min_id);
                return json_decode($data, 1);
            }
            return NULL;
        });
    }

    function pop() {
        return $this->do_lock(function () {
            //log::info('pop', 'max=' . $this->get_max_id(), 'min=' . $this->get_min_id());
            $max_id = $this->get_max_id();
            while ($max_id > 0 && $max_id >= $this->get_min_id()) {
                $data_key = $this->encode_key($max_id);
                if (!$this->exist($data_key)) {
                    if ($max_id > 0) {
                        $max_id = $this->decr_max_id();
                        continue;
                    }
                    return NULL;
                }
                $data = $this->get($data_key, 'data');
                $this->unset($max_id);
                return json_decode($data, 1);
            }
            return NULL;
        });
    }

    /**
     * @inheritDoc
     */
    public function offsetExists($offset) {
        return $this->meta_table->exist($this->encode_key($offset));
    }

    /**
     * @inheritDoc
     */
    public function offsetGet($offset) {
        $id = $this->meta_table->get($this->encode_key($offset), 'id');
        if (!$id) {
            return NULL;
        }
        $data = $this->get($this->encode_key($id), 'data');
        return $data ? json_decode($data, 1) : NULL;
    }

    /**
     * @inheritDoc
     */
    public function offsetSet($offset, $value) {
        $id = $this->meta_table->get($this->encode_key($offset), 'id');
        if (!$id) {
            return NULL;
        }
        return $this->set($this->encode_key($id), [
            'data' => json_encode($value, 320),
        ]);
    }

    /**
     * @inheritDoc
     */
    public function offsetUnset($offset) {
        return $this->do_lock(function () use ($offset) {
            return $this->unset($offset);
        });
    }

    /**
     * unset table
     *
     * @param $offset
     *
     * @return bool|null
     */
    public function unset($offset) {
        $encode_key = $this->encode_key($offset);
        $id         = $this->meta_table->get($encode_key, 'id');
        if (!$id) {
            return NULL;
        }
        $res = $this->del($this->encode_key($id));
        $res += $this->meta_table->del($encode_key);
        return $res;
    }
}