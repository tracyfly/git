#!/bin/bash
#Program:
#	2014-11-12
#	用于编译源代码
pa=/home/team50/doc/siming/src
pab=/home/team50/doc/siming/bin

#编译liblinear包
cd ~
cd $pa/de/bwaldvogel/liblinear
javac * -d ../../../../bin/

#编译cn.edu.ustc.ct的两个包
cd ~
cd $pa
javac -cp  ../lib/hadoop-core-1.2.1.jar -encoding GBK cn/edu/ustc/ct/vectorized/Caid.java  -d ../bin/
javac -cp  ../lib/hadoop-core-1.2.1.jar -encoding GBK cn/edu/ustc/ct/vectorized/Sample.java  -d ../bin/
javac -cp  ../lib/hadoop-core-1.2.1.jar:../bin/ -encoding GBK cn/edu/ustc/ct/lr/PredictProb.java  -d ../bin/
javac -cp  ../lib/hadoop-core-1.2.1.jar -encoding GBK cn/edu/ustc/ct/lr/TopID.java  -d ../bin/

cd ../bin
jar -cvf siming.jar *
cd ~
cp $pab/siming.jar /home/team50/siming.jar