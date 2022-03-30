#!/bin/bash
#shell中#后面的内容是注释，第一行是例外，第一行告诉shell使用什么来解析脚本
: << EOF
这里是多行注释，
: 是个空命令，注释被当作参数传入，但是什么都没做
EOF

#shell 会通过 PATH 环境变量来查找命令 /usr/bin /usr/sbin /home/root/bin /home/root/sbin

date 

whoami

chmod +x file.sh  #给文件增加执行权限

echo 

#变量名前面加$或者${HOME}来引用这个变量的值 ,如果要输出$，可以使用\$

#自定义变量用=赋值，变量、=、值之间不能出现空格，变量名区分大小写，只能使用字母、_、数字，长度不超过20。
#脚本中定义的变量生命周期未脚本结束，变量的数据类型会自动判断。
#变量名前面加$来引用变量的值，将一个值赋值给变量名时不需要$

#可以将命令的结果赋值给变量
#data_dt=`date`  |  data_dt=$(date)

#输出重定向
date > test    #>将内容覆盖到test文件中
date >> test  #>>将内容追加到test文件中，不会清楚原内容

#输入重定向
wc < test       #将文本内容重定向到命令中
wc << EOF       #阿巴阿巴



EOF

command1 | command2     #管道符将命令1的输出重定向到命令2中，这两个命令是同时执行，第一个输出立即作为第二个的输入，不会在中间文件或者缓冲区存储数据

#数学运算
expr  5 \* 2        #这种用法太麻烦，需要转义
$[2*3]

var5=$(bc << EOF        #bc 指定精度的计算结果
scale = 4
a1 = ($var1 * $var2)
b1 = ($var3 * $var4)
a1 + b1
EOF
)

var4=$(echo "scale=4; $var3 * $var2" | bc)



#变量
#变量类型：局部变量、全局变量、环境变量
#局部变量：在结构体中使用local 变量名='' 定义，此变量结构体结束即销毁
#全局变量：在shell中定义，在当前shell中可用，子shell中不可用
#环境变量：在所有的shell中可用

#变量操作
name='lily'     #创建变量 赋值符号两边不可以有空格 ，有空格使用引号包起来
name1=lily
echo $name1     #引用变量 使用${变量名} 来引用变量，直接使用$变量名容易引起歧义，
echo ${name}1
readonly name   #只读变量，不可修改
unset name      #删除变量，不能删除只读变量

#字符串变量 
#单引号 字符串只是字符串，引用变量失效 '${name}' 也只是字符串，不是引用name的值
#双引号 可以引用变量 "${name}" 使用的是name的值
#单引号和双引号嵌套的情况下，以最外层的为准
#拼接字符串 中间不能出现空格，有空格就分割了，赋值符号右边只有一个参数
name="111"aa"222"
echo ${#name}        #获取字符串长度
echo ${name:1:2}     #截取字符串 对变量从几号下标开始，截取几个字符。 下表从0开始


#参数传递
$0      #固定的文件名
$1      #引用传入的第1个参数
$n      #引用传入的第n个参数
$#      #传入的参数个数
$*      #以单个字符串的形式输出所有参数    "1 2 3 4"
$@      #以多个字符串的形式输出所有参数    "1" "2" "3" "4"
$$      #引用当前进程id
$?      #上条命令的执行状态  0为success，其他都为error
$!      #最后一个后台进程id


#数组 
array_name=(a b c d)    #使用小括号做边界，空格做分离
array_name[99]=g        #也可以单独定义下标元素的值，可以不连续
echo ${array_name[99]}  #引用数组元素的值
echo ${array_name[@]}   #输出数组所有元素，没有元素的下标省略
echo ${#array_name[@]}  #获取数组元素个数
echo ${#array_name[99]} #获取单个元素长度


#运算符
#算数运算       / 除法不只取整数，需要高精度使用bc进行计算
val=$[2*3]      #一般使用这种方法，不需要空格
val=$((2*3))    
let a+=a        #let 变量计算中不需要加上 $ 来表示变量

#数字关系运算符    只支持数字，不支持字符串，除非字符串内容只有数字    [ 和变量之间一定要有空格分离     return true | false
[ 1 -eq 2 ]     #比较是否相等，
[ 1 -ne 2 ]     #比较是否不相等 
[ 1 -gt 2 ]     #比较是否左边大于右边
[ 1 -lt 2 ]     #比较是否左边小于右边
[ 1 -ge 2 ]     #比较是否左边大于等于右边
[ 1 -le 2 ]     #比较是否左边小于等于右边

#字符串运算符     return true | false
[ 'qw' = 'q' ]  #检测两个字符串是否相等
[ 'qw' != 'q' ] #检测两个字符串是否不相等
[ -z 'qw' ]     #检测字符串长度是否为0
[ -n 'qw' ]     #检测字符串长度是否不为0
[ ${name} ]     #检测字符串是否不为空     有值返回true ""是没值

#布尔运算符      return true | false
[ !true ]                   #非运算，取反
[ 1 -eq 2 -o 'qw' = 'qw' ]  #或运算，有一个为true就返回true，都比较
[ 1 -eq 2 -a 'qw' = 'q' ]   #与运算，两个都为true才返回true，都比较

#逻辑运算符
[ 1 -eq 2 || 'qw' = 'qw' ]  #逻辑或运算，有一个为true就返回true，第一个为true就不比较第二个
[ 1 -eq 2 && 'qw' = 'q' ]   #逻辑与运算，两个都为true才返回true，第一个为false就不比较第二个

#文件运算符
[ -b $file ]         #检测文件是否是块设备文件，如果是，则返回 true。 
[ -c $file ]         #检测文件是否是字符设备文件，如果是，则返回 true。 
[ -d $file ]         #检测文件是否是目录，如果是，则返回 true。 
[ -f $file ]         #检测文件是否是普通文件（既不是目录，也不是设备文件），如果是，则返回 true。 
[ -g $file ]         #检测文件是否设置了 SGID 位，如果是，则返回 true。 
[ -k $file ]         #检测文件是否设置了粘着位(Sticky Bit)，如果是，则返回 true。 
[ -p $file ]         #检测文件是否是有名管道，如果是，则返回 true。 
[ -u $file ]         #检测文件是否设置了 SUID 位，如果是，则返回 true。 
[ -r $file ]         #检测文件是否可读，如果是，则返回 true。 
[ -w $file ]         #检测文件是否可写，如果是，则返回 true。 
[ -x $file ]         #检测文件是否可执行，如果是，则返回 true。
[ -s $file ]         #检测文件是否为空（文件大小是否大于0），不为空返回 true。
[ -e $file ]         #检测文件（包括目录）是否存在，如果是，则返回 true。


#执行相关
#命令替换
echo `ls /etc`       #反引号，先执行反引号内的命令，再将结果以参数的形式传入
echo $(ls /etc)      #$()效果等同于反引号，不是所有unix都有
path=$(cd `dirname $0`;pwd)  #`dirname $0` 列出当前脚本的相对路径，再进入这个目录，使用pwd获取执行文件的绝对路径


#字符串打印
echo "---"           #打印在控制台后自动换行
printf "---\n"       #打印在控制台后不换行，需要手动添加\n

#流程控制
#if else
#if fi
if condition; then 
    command1 command2 ... commandN
fi

#if else fi
if condition; then
    command1 command2 ... commandN
else
    command 
fi

#if else-if elseif 
if condition1; then
    command1 
elif condition2 then;
    command2
else 
    commandN 
fi

#for
for var in item1 item2 ... itemN 
do 
    command1 command2 ... commandN 
done

#while
a=1
while ((a<=10)) 
do 
    echo ${a}
    let a+=1
done
#经典用法
while read a
do
        echo $a
done < file_path/file_name

#until 循环执行一系列命令直至条件为 true 时停止。循环与 while 循环在处理方式上刚好相反。
until ((a<=10)) 
do
    command 
done

#case   case语句为多选择语句。可以用case语句匹配一个值与一个模式，如果匹配成功，执行相匹配的命令。
#       case需要一个esac（就是case反过来）作为结束标记，每个case分支用右圆括号，用两个分号表示break，其中“;;”不是跳出循环，是不在去匹配下面的模式
case 值 in 
模式1) 
    command1 command2 ... commandN 
;; 
模式2)
    command1 command2 ... commandN 
;; 
*)
    command1
;;
esac

#跳出循环
break       #跳出总循环
continue    #跳出当前循环，继续下一次循环
return      #结束当前方法并返回值

#定义函数       函数定义[ function ] funname() { action; [return int;] }
#参数传递
fun_name 2 3 4  #调用函数
#函数中使用：和shell取用函数相同 $n $# $* $? 或者加上{}
funWithParam(){ 
    echo "第一个参数为 $1 !" 
    echo "第二个参数为 $2 !" 
    echo "第十个参数为 $10 !" 
    echo "第十个参数为 ${10} !" 
    echo "第十一个参数为 ${11} !" 
    echo "参数总数有 $# 个!" 
    echo "作为一个字符串输出所有参数 $* !"
} 
funWithParam 1 2 3 4 5 6 7 8 9 34 73 
echo $?         # 判断执行是否成功

#函数返回值          
return [0-255]
: << EOF
return字样可存在也可不存在
return 只能为 return [0-255]，此处的返回可作为函数执行的状态，通过$?获取的便是这个返回值
如果不加return ， 则函数中所有的echo、printf输出组合成一个字符串作为BIN=`fun`的值
                  则默认最后一条语句的执行状态所为函数执行状态的返回值
                        如果最后一条语句执行成功，则$?为0，否则不为0
EOF

#使用函数返回值
fun_test(){
    echo "test_fun()"
}
fun_name=`fun_test`

echo ${fun_name}        # 此函数的echo输出会以空格分隔组合成一个字符串作为下述BIN的值 
: << EOF
return返回的数字，只是作为函数执行状态的返回值，也就是接下来$?获取的值
对于类似于上面的BIN=`fun_test`语句，获取的是函数体内所有的echo、printf输出组合成的一个字符串 
EOF


#输入输出重定向
: << EOF
一般情况下，每个 Unix/Linux 命令运行时都会打开三个文件：
* 标准输入文件(stdin)：stdin的文件描述符为0，Unix程序默认从stdin读取数据。
* 标准输出文件(stdout)：stdout 的文件描述符为1，Unix程序默认向stdout输出数据。
* 标准错误文件(stderr)：stderr的文件描述符为2，Unix程序会向stderr流中写入错误信息。
默认情况下，command > file 将 stdout 重定向到 file，command < file 将stdin 重定向到 file。
如果希望执行某个命令，但又不希望在屏幕上显示输出结果，那么可以将输出重定向到 /dev/null：
#输入重定向
1. bash.sh < file ： 将脚本的输入重定向到file，由file提供参数
#输出重定向
1. bash.sh > file ： 将脚本的输出数据重定向到file中，覆盖数据
2. bash.sh >> file ： 将脚本的输出数据重定向到file中，追加数据
3. command >> file 2>&1 ： 将 stdout 和 stderr 合并后重定向到 file
#读取外部输入
命令：`read arg` （脚本读取外部输入并赋值到变量上）
在shell脚本执行到上述命令时，停止脚本执行并等待外部输入，将外部输入赋值到arg变量上，继续执行脚本
#文件引用
引用其他的文件之后，可以使用其变量、函数等等，相当于将引用的文件包含进了当前文件
两种方式：
1. **.** file_path\file_name
2. **source** file_path\file_name
#颜色标识
printf "\033[32m SUCCESS: yay \033[0m\n";
printf "\033[33m WARNING: hmm \033[0m\n";
printf "\033[31m ERROR: fubar \033[0m\n";
EOF


#在shell中为避免一个语句过长，可以使用“\”进行换行
command arg1 \
arg2 \
arg3 
#上述语法等同于
command arg1 arg2 arg3


