<?php
/**
 * User: dj
 * Date: 2020/8/8
 * Time: 下午2:20
 * Mail: github@djunny.com
 */
include '../src/MProcess.php';
include '../src/QueueTable.php';

$tasks   = range(1, 10);
$process = new \ZV\MProcess(count($tasks), 1024 * 10);

foreach ($tasks as $id) {
    $process->push($id);
}
// start count($tasks) process loop
$process->loop(function ($task_data) use ($process) {
    $id  = (int)$task_data;
    $log = sprintf('[PID=%s][GotTask=%d]' . PHP_EOL, getmypid(), $id);
    echo $log;
    // do task

    // push new task
    $process->lock(function () use ($process, $log) {
        if ($process->incr('tasks') < 5) {
            foreach (range(1, 10) as $_) {
                $new_id = rand(1000, 9999);
                $process->push($new_id);
                echo $process->atomic('tasks') . '[NewTask=' . $new_id . '/' . $_ . ']' . $log;
            }
        }
    });
})->wait();
