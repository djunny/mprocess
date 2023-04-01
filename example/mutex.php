<?php

include '../src/MProcess.php';
include '../src/QueueTable.php';
$retry = 100;
while ($retry--) {
    $steps           = rand(1, 9) * (10 ** rand(1, 3));
    $zoom            = rand(3, 9);
    $process         = new ZV\MProcess(10, $steps, 64);
    $process['test'] = $steps;
    $process->on_init(function () use ($process, $steps) {
        for ($i = 0; $i < $steps; $i++) {
            $process->push(9999);
        }
    })->do(function ($index, $count) use ($zoom, $process, $steps) {
        if ($index == 0) {
            for ($i = 0; $i < $steps * ($zoom - 1); $i++) {
                $process->push(999);
            }
        }
        while (true) {
            // test for pop/shift
            $task = rand(0, 1) ? $process->shift() : $process->pop();
            //
            $task > 0 && $process->incr('result');
            if ($task === null) {
                break;
            }
        }
    })->wait();


    echo '======TestRound(' . $retry . ')======', PHP_EOL;

    if ($process->atomic('result') != $steps * $zoom) {
        echo 'Failed=', $process->atomic('result'), PHP_EOL;
    } else {
        echo 'Succed=', $process->atomic('result'), PHP_EOL;
    }
    if ($process['test'] != $steps) {
        echo 'KeyFailed', PHP_EOL;
    } else {
        echo 'KeySucced', PHP_EOL;
    }
    echo PHP_EOL;
}