<?php
/**
 * User: dj
 * Date: 2020/8/8
 * Time: 下午2:20
 * Mail: github@djunny.com
 */
include '../src/MProcess.php';
include '../src/QueueTable.php';

$process = new \ZV\MProcess(5);

$task_list = range(0, 10);

$process->do(function ($process_index, $process_count) use ($task_list) {

    foreach ($task_list as $task_index => $task) {

        if ($process_count > 1 && $task_index % $process_count != $process_index) {
            continue;
        }

        echo sprintf('[%d/%d][PID=%d][Start=%d]' . PHP_EOL, $process_index, $process_count, getmypid(), $task);

        sleep(rand(1, 3));

        echo sprintf('[%d/%d][PID=%d][Ended=%d]' . PHP_EOL, $process_index, $process_count, getmypid(), $task);
    }

})->wait();
