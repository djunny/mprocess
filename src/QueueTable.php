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
use Countable;
use ArrayAccess;

class QueueTable implements Countable, ArrayAccess {
    private $table;
    private $head;
    private $tail;
    private $ext_size;
    private $locker;
    private $data_size;
    private $size_cmp;
    private $size_set;
    private $size_max;
    private $pow;
    private $conflict = 1.5;

    public function __construct($size, $data_size) {
        $this->data_size = $data_size;
        $this->table     = $this->createTable($size);
        $this->head      = new Atomic(0);
        $this->tail      = new Atomic(0);
        $this->ext_size  = new Atomic(0);
        $this->locker    = new Lock(SWOOLE_MUTEX);
    }

    public function count() {
        return $this->table->count() - $this->ext_size->get(); // 减去 head 和 tail 两个元素
    }

    private function createTable($size) {
        // keep size is power of 2
        $this->pow = ceil(log($size, 2));
        $size      = 2 ** $this->pow;
        $this->size_set = $size;
        $this->size_cmp = $size * 0.7;
        $this->size_max = ceil($size * $this->conflict);
        $table          = new Table($this->size_max);
        $table->column('data', Table::TYPE_STRING, $this->data_size);
        //$table->column('tail', Table::TYPE_INT);
        $table->create();
        // copy table old data
        if ($this->table) {
            foreach ($this->table as $key => $item) {
                $table->set($key, $item);
            }
            //log::info('create new table, copy old data:' . $this->size_max, round(memory_get_usage(true) / 1024 / 1024, 2) . 'M');
        }
        return $table;
    }

    public function push($data) {
        $tail = $this->bulk(function () use ($data) {
            $tail = $this->tail->add();
            if ($this->table->count() >= $this->size_cmp) {
                // auto extend table size
                $this->table = $this->createTable($this->size_max);
            }
            return $tail;
        });
        $key  = $this->queueIndex($tail);
        if (!$this->table->set($key, ['data' => json_encode($data, true)])) {
            // log::info($this->table->count(), $this->size_max);
            throw new Exception('error:' . error_get_last()['message']);
        }
        return $tail;
    }

    public function pop() {
        return $this->bulk(function () {
            $tail = $this->tail->get();
            // log::info('tail: ' . $tail);
            if ($tail > 0) {
                $key    = $this->queueIndex($tail);
                $result = $this->table->get($key, 'data');
                if ($result === false) {
                    return null;
                }
                $this->table->del($key);
                $this->tail->sub();
                return $result === false ? $result : json_decode($result, true);
            }
            return null;
        });
    }

    public function shift() {
        return $this->bulk(function () {
            $head = $this->head->get();
            $tail = $this->tail->get();
            if ($head == $tail) {
                return null; // empty queue
            }
            $key   = $this->queueIndex($head + 1);
            $value = $this->table->get($key);
            if ($value === false) {
                return null;
            }
            $this->head->add(); // move head pointer
            $this->table->del($key); // remove element
            return json_decode($value['data'], true);
        });
    }

    public function bulk($callback) {
        try {
            while (!$this->locker->trylock()) {
                usleep(1000);
            }
            $res = $callback();
        } finally {
            $this->locker->unlock();
        }
        return $res;
    }

    public function offsetExists($key) {
        $index = $this->keyIndex($key);
        return $this->table->exists($index);
    }

    public function offsetGet($key) {
        $index = $this->keyIndex($key);
        return $this->table->get($index)['data'] ?? null;
    }

    public function offsetSet($key, $value) {
        return $this->bulk(function () use ($key, $value) {
            $index = $this->keyIndex($key);
            $this->ext_size->add();
            return $this->table->set($index, ['data' => json_encode($value, 320)]);
        });
    }

    public function offsetUnset($key) {
        return $this->bulk(function () use ($key) {
            $index = $this->keyIndex($key);
            if ($this->table->del($index)) {
                $this->ext_size->sub();
            }
        });
    }

    private function keyIndex($key) {
        if ($key === null) {
            throw new InvalidArgumentException('Key can not be null.');
        }
        return '@' . $key;
    }

    private function queueIndex($id) {
        return /*':' .*/ $id;
    }
}