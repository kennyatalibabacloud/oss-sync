# oss-sync
Sample java code showing the concept how to use Alibaba cloud [function compute](https://www.alibabacloud.com/product/function-compute) 
to implement [OSS](https://www.alibabacloud.com/product/oss) bucket sync across different bucket in different region

## Limitation
- example quality code, not for **production** use
- only handle file replication within the same access id/key
- handle `putobject` and `deleteobject` events only

## Todo
- handle multiple part upload sync
- handle cross account bucket replication
- better exception handling
