# mprocess

Multi process task execution based on swoole, support queue/lock/atomic

### Installation

```
composer require zv/mprocess
```

### basic

```php

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

```

### queue

```php

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

```

### loop task under multi process

```php

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

```

### locker && atomic

```php

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

```

### event

```php

$tasks   = range(1, 20);
$process = new \ZV\MProcess(count($tasks), 1024 * 10);
// start count($tasks) process loop
$process->on_init(function() use($tasks) {
    // init process by master process
    foreach ($tasks as $id) {
        $process->push($id);
    }
})->loop(function ($id) use ($process) {
    // push new task
    $process->lock(function () use ($process, $id) {
        if ($process->incr('tasks') < 5) {
            $log = sprintf('[PID=%s][GotTask=%d]' . PHP_EOL, getmypid(), $id);
            echo $log;
        }
    });
})->on_done(function(){
    // when all process work done
})->on_timeout(function(){
    // when process wait timeout
})->wait(600);// wait all process for 10 miniutes

```

### map && queue

```php

$tasks   = range(1, 20);
$process = new \ZV\MProcess(count($tasks), 1024 * 10, 64);

// set key index
$process['test'] = 1;

// push to queue
foreach ($tasks as $id) {
    $process->push($id);
}

// loop queue
$process->loop(function ($id) use ($process) {
    echo $id,PHP_EOL;
})->wait();

// keep in memory 
echo $process['test'] == 1, PHP_EOL;

```

more example see example/