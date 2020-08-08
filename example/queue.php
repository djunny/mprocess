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
foreach ($task_list as $task) {
    $process->push($task);
}

$process->do(function ($process_index, $process_count) use ($process) {

    while ($task = $process->pop()) {

        echo sprintf('[%d/%d][PID=%d][Start=%d]' . PHP_EOL, $process_index, $process_count, getmypid(), $task);

        sleep(rand(1, 3));

        echo sprintf('[%d/%d][PID=%d][Ended=%d]' . PHP_EOL, $process_index, $process_count, getmypid(), $task);
    }

})->wait();
