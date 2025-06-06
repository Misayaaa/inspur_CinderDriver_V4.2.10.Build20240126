说明
====
本文件夹包含以下内容
1. Cinder驱动包，名字必须以inspur_CinderDriver开头，目录中只能有一个待安装的驱动包。
2. setting文件，名字必须为setting，用于设置对接过程中使用的变量，
   如存储访问信息，OpenStack节点信息。
3. openstack_id_rsa，名字必须为openstack_id_rsa，访问各OpenStack节点所用的ssh私钥文件。
   可选，当存在时，使用该密钥访问OpenStack节点。

示例如下：
```
[root@lab data]# tree
.
├── inspur_CinderDriver_V4.2.4.Build20220812.tar.gz
├── setting
├── openstack_id_rsa
├── README

1 directories, 4 files
```
