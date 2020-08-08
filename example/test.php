<?php
/**
 * User: dj
 * Date: 2020/8/8
 * Time: 上午1:34
 * Mail: github@djunny.com
 */

include '../src/MProcess.php';
include '../src/QueueTable.php';


$loop       = 0;
$all_tasks  = 0;
$start_time = microtime(1);
while ($loop++ <= 100) {
    $process_count = 50;
    $process       = new \ZV\MProcess($process_count);

    $task_count = rand(1, $process_count);

    $process->do(function ($index, $count) use ($process, $task_count) {
        $tasks = 100;
        while ($index < $task_count && $tasks-- > 0) {
            if (!$process->push([$index, $count, $tasks])) {
                throw new Exception('TaskPushError');
            } else {
                $process->incr('tasks');
            }
        }
        while (TRUE) {
            if (rand(1, 2) == 1) {
                $data = $process->pop();
            } else {
                $data = $process->shift();
            }
            if ($data === NULL) {
                break;
            }
            // do task

            $process->incr('done');
        }
    })->wait();

    $need_count = $process->atomic('tasks');
    $done_count = $process->atomic('done');
    $all_tasks  += $need_count;
    $memory     = memory_get_usage();
    if ($done_count != $need_count || count($process) != 0) {
        echo 'TestFailed',
        '[count=', $loop, ']',
        '[', $done_count . '!=' . ($need_count), ']',
        '[process=', count($process), ']',
        '[memory=', ($memory / 1024 / 1024) . 'M]', PHP_EOL;
        exit;
    }
    //exit;
}
$use_time = round(microtime(1) - $start_time, 2);
$qps      = round(($all_tasks / $use_time));
echo '[Result]', '[Tasks=', $all_tasks, ']',
    '[Time=' . $use_time . 's]',
    '[QPS=' . $qps . '/s]';
exit;