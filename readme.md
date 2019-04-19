# 支付系统对账脚本

用python写的针对第三方支付机构的对账脚本。

主要流程就两步：

1. 向第三方支付机构（这里是乐刷)发起支付请求，下载对账单
2. 根据下载的对账单数据与本地的数据库订单数据进行对账

具体的对账逻辑涉及费率，收益等计算比较复杂，这部分是由存储过程处理的，python只是调用存储过程的函数。

