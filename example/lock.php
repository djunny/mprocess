<?php
/**
 * User: dj
 * Date: 2020/8/8
 * Time: 下午2:58
 * Mail: github@djunny.com
 */
include '../src/MProcess.php';
include '../src/QueueTable.php';

$tasks   = range(1, 20);
$process = new \ZV\MProcess(count($tasks), 1024 * 10);

foreach ($tasks as $id) {
    $process->push($id);
}
// start count($tasks) process loop
$process->loop(function ($id) use ($process) {
    // push new task
    $process->lock(function () use ($process, $id) {
        if ($process->incr('tasks') < 5) {
            $log = sprintf('[PID=%s][GotTask=%d]' . PHP_EOL, getmypid(), $id);
            echo $log;
        }
    });
})->wait();
