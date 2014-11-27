#!/bin/bash
#Program:
#	2014-11-13
#	执行脚本
#	验证集结果为0.2675
pa=/user/team50/final

#以caid特征生成libsvm格式的训练集
status=1
while [ "$status" -ne "0" ]
do
	hadoop jar siming.jar cn.edu.ustc.ct.vectorized.Caid /data/train/monitorData/part* $pa/train/train1
	status=$?
done

#以caid特征生成libsvm格式的验证集
status=1
while [ "$status" -ne "0" ]
do
	hadoop jar siming.jar cn.edu.ustc.ct.vectorized.Caid /data/validation/monitorData/part* $pa/validation/test1
	status=$?
done

#以caid特征生成libsvm格式的测试集
status=1
while [ "$status" -ne "0" ]
do
	hadoop jar siming.jar cn.edu.ustc.ct.vectorized.Caid /data/test/monitorData/part* $pa/test/test1
	status=$?
done

#在训练集中按照1:500的正负样本比抽取数据用于建模
status=1
while [ "$status" -ne "0" ]
do
	hadoop jar siming.jar cn.edu.ustc.ct.vectorized.Sample /data/train/transformData/part-00000 /data/train/transformData/part-00001 $pa/train/train1/part* $pa/train/train2 500
	status=$?
done

#对验证集预测得到按照转化率降序排名
status=1
while [ "$status" -ne "0" ]
do
	hadoop jar siming.jar cn.edu.ustc.ct.lr.PredictProb $pa/train/train2/part-r-00000 $pa/validation/test1/part* $pa/result_v 0.1
	status=$?
done

#对测试集预测得到按照转化率降序排名
status=1
while [ "$status" -ne "0" ]
do
	hadoop jar siming.jar cn.edu.ustc.ct.lr.PredictProb $pa/train/train2/part-r-00000 $pa/test/test1/part* $pa/result_t 0.1
	status=$?
done

#取验证集结果topk
status=1
while [ "$status" -ne "0" ]
do
	hadoop jar siming.jar cn.edu.ustc.ct.lr.TopID  $pa/result_v/part-r-00000 $pa/validation/submit 350
	status=$?
done

#取测试集结果topk
status=1
while [ "$status" -ne "0" ]
do
	hadoop jar siming.jar cn.edu.ustc.ct.lr.TopID  $pa/result_t/part-r-00000 $pa/test/submit 350
	status=$?
done

#覆盖solution_v
status=1
while [ "$status" -ne "0" ]
do
	hadoop fs -cp -f $pa/validation/submit/part-r-00000 /user/team50/solution_v
	status=$?
done

#覆盖solution_t
status=1
while [ "$status" -ne "0" ]
do
	hadoop fs -cp -f $pa/test/submit/part-r-00000 /user/team50/solution_t
	status=$?
done

#验证集刷榜
status=1
while [ "$status" -ne "0" ]
do
	submit
	status=$?
	if [ "$status" -ne "0" ];then
		sleep 300
	fi
done
