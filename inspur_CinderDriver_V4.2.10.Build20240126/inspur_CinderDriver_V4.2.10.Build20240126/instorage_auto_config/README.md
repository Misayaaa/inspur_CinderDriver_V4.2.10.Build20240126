使用限制
========
1. 当前仅支持IPv4环境。
2. 需要根据使用说明进行操作。

存储准备
========
1. 存储系统需要完成部署及初始化。
2. 存储系统与OpenStack集群网络互通。
3. 存储上已完成待对接存储池的创建。

使用说明
========
1. 本对接工具运行的节点称为部署节点，部署节点需要能够免密OpenStack节点，通常建议将该对接工具放在openstack其中一个控制节点中执行。
2. 准备data目录，data目录中的setting文件，定义了对接参数，可参考
   配置项参数说明部分。data目录内容参考data目录中的README文件。
3. 可以通过./checkparam.sh ./data命令检查存储环境，期间为了能在存储中执行命令，需要输入存储密码。
4. 可以通过./checkenv.sh ./data对待对接的OpenStack环境进行检查，
   获取OpenStack相关服务运行状态。
5. 可以通过./backup.sh ./data [suffix] 对OpenStack各节点部署
   对接过程中涉及修改的目录及文件进行备份，备份后缀中包括suffix。
6. 可以通过./configure.sh ./data对各OpenStack控制节点进行对接配置。
7. 可以通过./cleanup.sh ./data [suffix]对指定的备份内容进行清除。
8. 可以通过./restore.sh ./data [suffix]对指定的备份内容进行恢复。

对接后操作
==========
1. 需要在OpenStack端根据后端类型创建相应的存储卷类型供后续使用。
   卷类型中的volume_backend_name参数同setting文件中的存储后端名。

配置项参数说明
==============

配置项名称 | 类型 | 说明
-----------|------|-----
STORAGE_PROTOCOL | 字符串 | 对接存储使用的协议类型，支持ISCSI,FC,RDMA。
STORAGE_IP | 字符串 | 存储集群IP。
STORAGE_USERNAME | 字符串 | 访问存储时使用的用户名。
STORAGE_PASSWORD | 字符串 | 访问存储时使用的密码。
STORAGE_POOL_NAME | 字符串 | 用于对接Cinder服务的存储池。
STORAGE_BACKEND_NAME | 字符串 | 存储后端名称。
OPENSTACK_CONTROLLER_NODES | 空格分隔的IP地址 | OpenStack各节点管理IP，通过空格分隔。
OPENSTACK_KEYSTONRC_PATH | 字符串 | OpenStack节点上keystonerc文件的绝对路径。
