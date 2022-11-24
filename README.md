# NaiveBayesClassification
(公式均使用Typora内置latex书写，建议下载观看）
使用Mapreduce实现朴素贝叶斯分类器

## 一、Project内容

1. 用MapReduce算法实现贝叶斯分类器的训练过程，并输出训练模型；
2. 用输出的模型对测试集文档进行分类测试。测试过程可基于单机Java程序，也可以是MapReduce程序。输出每个测试文档的分类结果；
3. 利用测试文档的真实类别，计算分类模型的Precision，Recall和F1值。

## 二、贝叶斯分类器理论介绍

### Bayes公式

计算条件概率p（c~i~ | d）时，概率论中有如下Bayes公式：
$$
p(c_i|d)=\frac{p(c_i,d)}{p(d)}=\frac{p(d|c_i)p(c_i)}{p(d)}
$$
Bayes公式讲述了以下信息：
$$
p(c_i|d)=\frac{p(d|c_i)p(c_i)}{p(d)}\to posterior = \frac{likelihood \times prior}{evidence} \\
p(c_i|d):后验概率或条件概率(posterior) \quad p(c_i):先验概率(prior) \\
p(d|c_i):似然概率(likelihood) \qquad \quad p(d):证据(evidence)
$$
Bayes公式的意义：

* 当观察到evidence p(d)时，后验概率p（c~i~ | d）取决于似然概率p(d | c~i~)和先验概率p（c~i~）。因为当evidence p(d)已知时，p（d）成为变量，Bayes公式变成：
  $$
  p(c_i|d)=\frac{p(d|c_i)p(c_i)}{p(d)}\propto p(d | c_i)p(c_i) \\符号\propto表示成正相关 \\
  因此我们实际上关心的是p（c_i|d）的相对大小
  $$

### 朴素贝叶斯方法（Naive Bayes）

再说到Naive Bayes分类器本身：
$$
p(c_i|d)=\frac{p(d|c_i)p(c_i)}{p(d)}\propto p(d | c_i)p(c_i)
$$
对于类标签集合C中的每个类标签c~i~(i = 1,…， j)，计算条件概率p（c~i~ | d)

* 使条件概率p（c~i~ | d)最大的类别作为文档d最终的类别
  $$
  c_d=\mathop{\arg\max}\limits_{c_i\in C}\ p(d | c_i)p(c_i)
  $$

* 其中c~d~为文档d被赋予的类型，c~d~=使得条件概率p（c~i~ | d)最大的类型。

  根据Bayes公式，c~d~=使得p（d | c~i~) p（c~i~)值最大的类型

接下来考虑的便是如何计算得到p（d | c~i~)和p（c~i~)？

* 对于Navie Bayes而言，此时用训练集对机器进行训练就是为了算出p（d | c~i~)和p（c~i~)。
* 训练的过程就是参数估计的过程。这里要估计的参数就是p（d | c~i~)和p（c~i~)。

### Naive Bayes的参数估计

假设类别标签集合C = {c~1~，c~2~，...，c~j~}

假设训练集D包含N个文档，其中每个文档都被标上了类别标签

首先估计先验概率p（c~i~）（i = 1，...，j）：
$$
p(c_i)=\frac{类型为c_i的文档个数}{训练集中文档总数N}
$$
还需要估计似然概率p（d | c~i~)，需要一个假设：Term独立性假设

* 文档中每个term的出现时彼此独立的

基于这个假设，似然概率p（d | c~i~）的估计方法如下：

* 假设文档d包含n~d~个term：t~1~，t~2~，…，t~nd~

* 根据Term的独立性假设，有
  $$
  p(d\ |\ c_i)=p(t_1,t_2,...,t_nd\ |\ c)=p(t_1\ | \ c)p(t_2\ | \ c)\cdots p(t_nd\ | \ c)\\
  =\prod\limits_{1\le k \le n_d}p(t_k\ |\ c_i )
  $$

* 因此，估计p（d | c~i~）就需要估计p（t~k~ | c~i~）
  $$
  p(t_k\ |\ c_i )=\frac{t_k在类型为c_i的文档中出现的次数}{在类型为c_i的文档中出现的term的总数}
  $$

### 朴素贝叶斯分类器（Navie Bayes Classifier）

朴素贝叶斯分类器是一个概率分类器，通过概率值的相对大小来进行分类

我们的目标就是找到最好的类别，也就是通过计算获取到最大概率对应的类别
$$
P(c\ |\ d) \propto P(c)\ \prod\limits_{1\le k \le n_d} P(t_k\ |\ c) \\
c_{map} = \arg \max \limits_{c\in C}\hat{P}(c\ |\ d)=\arg \max \limits_{c\in C}\hat{P}(c) \prod\limits_{1\le k \le n_d} \hat{P}(t_k\ |\ c) \\
\hat{P}是P在训练集下得到估计值
$$
接着为了计算方便，运用log（xy）= log（x）+ log（y）来进行计算上的简化。

因为log是一个单调函数，因此进行log处理后，各概率值的相对大小并不会发生改变，因此对我们最后的分类也不会产生影响。

即使我们可以将计算式改写为：
$$
c_{map} =\arg \max \limits_{c\in C}\ [\log\hat{P}(c) +  \prod\limits_{1\le k \le n_d} \log \hat{P}(t_k\ |\ c)] \\
\hat{P}(c)=\frac{N_c}{N}\quad (N_c:文档类型总数，N:总文档数）\\
\hat{P}(t\ |\ c)=\frac{T_{ct}}{\sum_{t'\in V}T_{ct'}} \\
T_{ct}：term在对应训练集文档类型c的文档中出现总次数
$$

### 加入一些平滑系数

有些出现在测试集中term并未在训练集中出现过，导致概率值直接变为0的情况，因此需要加入一下平滑系数来避免这种情况的发生。

于是便有：
$$
\hat{P}(t\ |\ c)=\frac{T_{ct}+1}{\sum_{t'\in V}(T_{ct'}+1)}=\frac{T_{ct}+1}{\sum_{t'\in V}T_{ct'}+B} \\
B为训练集中出现的单词种类总数
$$

## 三、数据集说明

使用的是Industry 文件集下的I01001与I13000

| 数据集\类别 | I01001 | I13000 | 总文档数 |
| :---------: | :----: | :----: | :------: |
|   训练集    |  108   |  195   |   303    |
|   测试集    |   72   |  130   |   202    |
|  总文档数   |  180   |  325   |   505    |

### 
