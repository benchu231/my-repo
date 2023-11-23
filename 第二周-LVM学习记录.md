# LVM学习记录



## 什么是LVM?

​	LVM是 Logical Volume Manager（逻辑[卷管理](https://baike.baidu.com/item/卷管理?fromModule=lemma_inlink)）的简写，它是Linux环境下对[磁盘分区](https://baike.baidu.com/item/磁盘分区/1521981?fromModule=lemma_inlink)进行管理的一种机制。

​	普通的磁盘分区管理方式在逻辑分区划分好之后就无法改变其大小，当一个逻辑分区存放不下某个文件时，这个文件因为受上层文件系统的限制，也不能跨越多个分区来存放，所以也不能同时放到别的磁盘上。而遇到出现某个分区空间耗尽时，除了**下线当前使用的硬盘**，安装上新的磁盘，将原始数据导入外，其他解决的方法通常是使用**符号链接**，或者使用**调整分区大小的工具**，但这只是暂时解决办法，没有从根本上解决问题。而且对于运行着程序的服务器来讲，停机并不是一件无关紧要的事情，使用调整分区大小的工具也会使整个存储变得不稳定，增加文件丢失损坏的纪律。但是随着Linux的逻辑卷管理功能的出现，这些问题都迎刃而解，用户在无需停机的情况下可以方便地调整各个分区大小。

## LVM的实现

​	LVM 逻辑卷管理通过将底层物理硬盘抽象封装起来，以逻辑卷的形式表现给上层系统，逻辑卷的大小可以动态调整，而且不会丢失现有数据。新加入的硬盘也不会改变现有上层的逻辑卷。作为一种**动态磁盘管理机制**，逻辑卷技术大大提高了磁盘管理的灵活性

​	**LVM的4个基本概念**

- ​	PE( Physical Extend ) 物理拓展
- ​	PV( Physical Volume ) 物理卷
- ​	VG( Volume Group ) 卷组
- ​	LV( Logical Volume ) 逻辑卷

​	在逻辑卷创建成功之前，会先将要作为逻辑卷使用的**物理磁盘**格式化(条带化)为物理卷**PV**,这个过程相当于将物理磁盘切割成多个存储块**PE**，每个PE默认为4MB，是**分配存储的基本单位**。格式化后，可以将一个或多个物理卷合为一个卷组**VG**，PV中的PE全部进入VG中，VG中包含PV切割好的存储块，相当于**存储池**。VG创建好之后，就可以分配PE，创建逻辑卷**LV**，LV中的PE可以来自不同的物理磁盘，而LV的扩充和缩减实际上就是其中PE的增加和减少，其过程不会丢失原始数据。

![](https://github.com/benchu231/imgs/blob/img/img/202311222232156.png)

​	LVM动态扩容实际上就是向VG中添加PE，通过分配新增的PE到LV来实现大小的调整。

![](https://github.com/benchu231/imgs/blob/img/img/202311222300458.png)

## CentOS上LVM的使用

在VMware中为虚拟机新增两块虚拟硬盘

**添加硬盘**

![](https://github.com/benchu231/imgs/blob/img/img/202311222309632.png)

![](https://github.com/benchu231/imgs/blob/img/img/202311222311746.png)

**添加成功**

![](https://github.com/benchu231/imgs/blob/img/img/202311222313018.png)

在系统内查看是否增加两块硬盘

```bash
fsidk -l
```

可以看到已经有两块新的硬盘装载(**/dev/sdb**  **/dev/sdc**)

![](https://github.com/benchu231/imgs/blob/img/img/202311222317226.png)

### 如何创建逻辑卷LV

1.将物理磁盘格式化为物理卷

```bash
pvcreate /dev/sdb /dev/sdc
```

![](https://github.com/benchu231/imgs/blob/img/img/202311222320445.png)

2.创建卷组，将pv加入该卷组中，卷组需要一个命名

```bash
vgcreate vg1 /dev/sdb /dev/sdc
```

![](https://github.com/benchu231/imgs/blob/img/img/202311222322052.png)

3.使用该卷组创建LV

```bash
lvcreate -n lv1 -L 2G vg1
```

报错，VG由于某些原因空余空间小于2G，创建失败。

![](https://github.com/benchu231/imgs/blob/img/img/202311222325268.png)

尝试创建1G的LV

创建成功

![](https://github.com/benchu231/imgs/blob/img/img/202311222327533.png)

将创建好的逻辑卷挂载使用

```bash
mount [逻辑卷路径] [挂载路径] #挂载路径作为逻辑卷存储路径 相当于Windows下的新加卷
mount /dev/vg1/lv1 /mnt
```

报错

![](https://github.com/benchu231/imgs/blob/img/img/202311222331009.png)

发现是创建的逻辑卷忘记初始化

进行初始化，创建文件系统

```
mkfs.ext4 /dev/vg1/lv1
```

格式化成功

![](https://github.com/benchu231/imgs/blob/img/img/202311222333623.png)

再次尝试挂载

成功

![](https://github.com/benchu231/imgs/blob/img/img/202311222335444.png)

LVM的信息查询

```bash
pvdisplay #查看pv的详细信息
pvs       #查看pv的简略信息 下同
vgdisplay
vgs
lvdisplay
lvs
```

![](https://github.com/benchu231/imgs/blob/img/img/202311222346565.png)

### 如何删除LVM

1.首先取消LV 在/mnt的挂载

```bash
umount /mnt/
mount #查看当前全部的挂载
```

可以看到已经没有 lv1的挂载了

![](https://github.com/benchu231/imgs/blob/img/img/202311230004044.png)

2.删除LV

```
lvremove /dev/vg1/lv1
lvs
```

lv删除成功

![](https://github.com/benchu231/imgs/blob/img/img/202311230006614.png)

3.删除VG

```
vgremove vg1
vgs
```

vg删除成功

![](https://github.com/benchu231/imgs/blob/img/img/202311230007029.png)

4.删除PV

```
pvremove /dev/sdb /dev/sdc
pvs
```

pv删除成功

![](https://github.com/benchu231/imgs/blob/img/img/202311230008114.png)

### lvm的拉伸与缩容

逻辑卷的拉伸操作可以在线执行。不需要卸载逻辑卷

**lvm的拉伸**

先查看要扩展的LV所属的VG是否有足够的free space来扩展LV

![](https://github.com/benchu231/imgs/blob/img/img/202311231045091.png)

![](https://github.com/benchu231/imgs/blob/img/img/202311231044096.png)

现在要对my_lv扩展2G，可以看到my_vg只有1020MB剩余空间，需要先对vg进行扩展

```bash
vgextend [卷组名] [物理卷名]
vgextend my_vg /dev/sdc
```

![](https://github.com/benchu231/imgs/blob/img/img/202311231048020.png)

my_vg剩余空间2.99G

![](https://github.com/benchu231/imgs/blob/img/img/202311231049889.png)

扩展LV

```bash
lvextend -L +[扩展大小] [逻辑卷路径]
lvextend -L +2G /dev/my_vg/my_lv
```

![](https://github.com/benchu231/imgs/blob/img/img/202311231056759.png)

![](https://github.com/benchu231/imgs/blob/img/img/202311231056623.png)

逻辑卷扩容成功，但文件系统显示仍为1G

```bash
df -h
```

![](https://github.com/benchu231/imgs/blob/img/img/202311231057126.png)

需要更新文件系统

```
resize2fs /dev/my_vg/my_lv
```

![](https://github.com/benchu231/imgs/blob/img/img/202311231058521.png)

由于是动态拉伸逻辑卷大小，并未下线逻辑卷，在其上层的文件系统与正在使用该卷的程序并不需要暂时下线，且扩容后，卷中原先存储的文件数据并不会丢失

**LVM缩容**

LVM缩容不像扩容一样，能够在线执行，随容需要将逻辑卷卸载下线后才能进行，以保证数据的安全性

取消目标LV的挂载

```
umount [逻辑卷路径]
umount /dev/my_vg/my_lv
```

![](https://github.com/benchu231/imgs/blob/img/img/202311232000889.png)

检查文件系统

```bash
e2fsck -f /dev/my_vg/my_lv # -f为遇见错误直接修复
```

![](https://github.com/benchu231/imgs/blob/img/img/202311232003590.png)

缩小文件系统

```
resize2fs /dev/my_vg/my_lv 1G
```

![](https://github.com/benchu231/imgs/blob/img/img/202311232008216.png)

缩小LV

```
lvreduce -L -1G /dev/my_vg/my_lv
```

![](https://github.com/benchu231/imgs/blob/img/img/202311232009020.png)

挂载

```
mount /dev/my_vg/my_lv /mnt
```

![](https://github.com/benchu231/imgs/blob/img/img/202311232012587.png)



## Linstor创建存储池

在所有的satellite上新建一个VG，最好所有节点的VG都用同一个命名

```
pvcreate /dev/sdb /dev/sdc #格式化物理磁盘
vgcreate vg_ssd /dev/sdb /dev/sdc #将两个PV放入VG，创建VG
```

在任意节点使用linstor 命令行创建linstor存储池

```bash
linstor sp create lvm node1 pool_ssd vg_ssd
linstor sp create lvm [节点名称] [存储池名称] [卷组名称] #通过lvm创建linstor存储池
```

![](https://github.com/benchu231/imgs/blob/img/img/202311232024523.png)

创建成功

![](https://github.com/benchu231/imgs/blob/img/img/202311232025661.png)

### 多节点共享存储池

查看两个satellite VG的UUID

![](https://github.com/benchu231/imgs/blob/img/img/202311232032073.png)

创建存储池

```bash
linstor storage-pool create lvm --external-locking \
--shared-space O1btSy-UO1n-lOAo-4umW-ETZM-sxQD-qT4V87 \
alpha pool_ssd shared_vg_ssd
linstor storage-pool create lvm --external-locking \
--shared-space O1btSy-UO1n-lOAo-4umW-ETZM-sxQD-qT4V87 \
bravo pool_ssd shared_vg_ssd
```

![](https://github.com/benchu231/imgs/blob/img/img/202311232038907.png)

### 直接通过物理磁盘创建存储池

这个方法需要满足以下四点要求

- 磁盘设备的大小必须大于1GB
- 这个磁盘设备必须是根设备，无子文件，如`/dev/vda` `/dev/sda`
- 这个磁盘设备上没有任何的文件系统或者blkid标记 (可以使用wipefs -a清除磁盘所有签名)
- 这个磁盘设备不是一个DRBD设备

为虚拟机新增两块磁盘，不做任何处理（不格式化为物理卷）

使用linstor查询可用的物理存储

```bash
linstor physical-storage list
```

有两个节点可用，每个节点都有两块可用物理磁盘

![](https://github.com/benchu231/imgs/blob/img/img/202311232055115.png)

创建存储池

```
linstor physical-storage create-device-pool --pool-name [逻辑卷名] \
LVMTHIN [节点名] [设备名] --storage-pool [Linstor存储池名]
linstor physical-storage create-device-pool --pool-name lv_my_pool \
LVMTHIN node1 /dev/sdb --storage-pool newpool
```

创建成功

![](https://github.com/benchu231/imgs/blob/img/img/202311232111650.png)

![](https://github.com/benchu231/imgs/blob/img/img/202311232114361.png)

可以看到实际上就是在对应节点格式化了磁盘，创建卷组，创建逻辑卷加入存储池

扩展存储池

![](https://github.com/benchu231/imgs/blob/img/img/202311232122458.png)

![](https://github.com/benchu231/imgs/blob/img/img/202311232123371.png)
