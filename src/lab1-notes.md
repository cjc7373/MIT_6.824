reduce 和 map 同时进行，那么 reduce 可能会在某个时刻阻塞
故一个 worker 不应只运行一个任务？错误的
或者 reduce 应在 map 全部结束之后运行? 正确的
reduces can't start until the last map has finished

- The worker's map task code will need a way to store intermediate key/value pairs in files in a way that can be correctly read back during reduce tasks?
hint 中写的是用 json 来存
我感觉也可以直接纯文本, 每行存一个 key value

还需要维护任务状态, 比如开始时间, 然后周期性检查..

worker 需要一些 metadata, 比如 map 和 reduce 节点的数量

需要知道 worker 的状态嘛? 还是知道 task 的状态就够了?

map 之后的排序/写入对应的文件需要 mapreduce lib 来处理?

## Some commands
- go build -race -buildmode=plugin ../mrapps/wc.go && go run -race mrworker.go wc.so