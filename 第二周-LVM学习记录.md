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

![](https://github.com/benchu231/imgs/blob/img/img/202311230024523.png)

创建成功

![](https://github.com/benchu231/imgs/blob/img/img/202311230025661.png)