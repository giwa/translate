これは私のApache Sparkのアーキテクチャに関する2本目の記事です。今回は、Sparkデザインの中でももっと面白いshuffleについて詳しく紹介していこうと思います。前回の記事は、Sparkのアーキテクチャとメモリ管理についてでした。

TODO: insert images

一般的にshuffleとはなんでしょうか？ 電話の詳細の履歴のリストがテーブルにあると思ってください。そして、それぞれの日にどのくらい電話があったかを数える場合を考えてください。この場合は、各レコードを"day"をキーにして1をバリューとして変換します。この処理の後、各キーごとのバリューを合計することで答えが得られます。しかし、データがクラスターに分散して保存されている場合、どのように同じキーで違うマシンに保存されたバリューを足し合わせるでしょうか？これを行うたった一つの方法は、同じキーを持つデータを同じマシンに集めることです。この処理の後データは足し合わせることができるようになるでしょう。

クラスターをまたがってデータをshuffleするには、様々なタスクが必要とされます。たとえば、"id"というフィールドで2つのテーブルをジョインするとき、同じidの値を持つすべてのデータが同じチャンクに保存されていないといけません。テーブルに1から1000000の範囲で整数のキーが存在するときを考えましょう。キーが1-100のデータがパーティションとして一つのチャンクに保存されていると仮定しましょう。そして、同じチャンクのデータをソートすることによってパーティション同士を直接ジョインすることができます。なぜならキーが1-100のデータはこれらの2つのパーティションに保存されていることがわかっているからです。これを実現されるには、2つのテーブルに同じだけのパーティションがなければなりません。この方法によって、ジョインは計算コストが少なくできます。さて、これでshuffleの重要性がわかったと思います。

このトピックを議論するにあたり、MapReduceの命名規則にのって話します。suffle実行する際には、source executorとして、データを放出するものを"mapper"と呼び。そのデータを消費するtarget executorを"reducer"と呼びます。これらのmapperとreducerの間で行われるのが"shuffle"です。

Shuffleは一般的に2つの重要なパラメータを取ります。`spark.shuffle.compress` shuffleのアウトプットを圧縮するかどうか。`spark.shuffle.spill.compress` Shuffleでメモリからあふれた中間ファイルを圧縮するかどうか。両者ともデフォルトで`true`になっています。`spark.io.compression.codec`を両者とも使っています。データを圧縮するためのcodecはデフォルトではsnappyです。

ご存知かも知れませんが、Sparkには数種類のshuffleの実装がされています。`spark.shuffle.manager`のパラメータによってそれらのshuffleを使うことができます。使えるShuffleの実装はhash, sort, tungsten-sortです。1.2からのデフォルトは"sort"になっています。

TODO: inset image of Hash Shuffle

Spark 1.2.0より前は、hashがデフォルトでした。しかし、hashは多くの欠点がありました。それらは、それぞれのmapperタスクがそれぞれのreducerのために作る分割ファイルの量は、クラスターのなかのM * R だけの数になります。（Mはmapperの数、Rはreducerの数です。）多くのmapperとreducerは大きな問題を生じます。mapperとreducerのbufferサイス、ファイルシステムの中でオープンされているファイルの数、それらのファイルを作成、削除する速さの問題などです。いい例としてはYahooが実際にこの問題に直面した時の例です。彼らは46kのmapperと46kのreducerを使い20億のファイルをクラスターの中で作成しました。

このShuffleの仕組みは賢いものではありません。reduce側でreducerの数をパーティションの数として数え、それらに対して別々のファイルを作成しアウトプットが必要なレコードをループします。target partiitionをそれぞれのreducerのために数え、対応したファイルに結果を書き出します。

TODO: insert an image of spark_hash_shuffle_no_consolidation

There is an optimization implemented for this shuffler, controlled by the parameter “spark.shuffle.consolidateFiles” (default is “false”). When it is set to “true”, the “mapper” output files would be consolidated. If your cluster has E executors (“–num-executors” for YARN) and each of them has C cores (“spark.executor.cores” or “–executor-cores” for YARN) and each task asks for T CPUs (“spark.task.cpus“), then the amount of execution slots on the cluster would be E * C / T, and the amount of files created during shuffle would be E * C / T * R. With 100 executors 10 cores each allocating 1 core for each task and 46000 “reducers” it would allow you to go from 2 billion files down to 46 million files, which is much better in terms of performance. This feature is implemented in a rather straightforward way: instead of creating new file for each of the reducers, it creates a pool of output files. When map task starts outputting the data, it requests a group of R files from this pool. When it is finished, it returns this R files group back to the pool. As each executor can execute only C / T tasks in parallel, it would create only C / T groups of output files, each group is of R files. After the first C / T parallel “map” tasks has finished, each next “map” task would reuse an existing group from this pool.

TODO: re-translate
このshuffleには最適化した実装があります。それは、"spark.shuffle.consolidateFiles"というパラメータで管理することができます。(デフォルトでは"false")　それが"true"になった時、"mapper"の出力ファイルは統合されます。もしクラスターがE個のエグゼキューター(YARNでは"-num-executors") とそれらが持つC個のコア("spark.executor.cores" か YARNでは"-executor-cores")と、それぞれのタスクのためのT個CPU("spark.task.cpus") とするとクラスターの実行されるスロットの数はE * C / Tになります。そしてShuffleの間に作られるファイルはE * C / T * Rとなります。10個のコアと100個のエグゼキューターで、それぞれが1コアあたりそれぞれのタスクに割り当てると46000の"reducers"では20億のファイルから4600万のファイルまで減らすことができます。これはパフォーマンス的にはかなりよい結果となります。この機能は新しくファイルをreducerのために作るのではなく、作られたファイルをプールすることによって実装されています。この処理が終わると、R個のファイルグループをプールに返します。それぞれのエグゼキューターではC / T個のタスクが並列で実行されます。それらは各グループにはR個のファイルがあるC / T個のグループのを作ります。 最初のC / Tの"map"処理が終わったあと次の"map"タスクが行われ、すでにあるグループのファイルを再利用します。


Here’s a general diagram of how it works:

spark_hash_shuffle_with_consolidation

長所:
No memory overhead for sorting the data;
No IO overhead – data is written to HDD exactly once and read exactly once.
高速: ソートは必要ない。ハッシュテーブルを維持する必要が無い
ソートするためのメモリオーバーヘッドがない。
IOのオーバーヘッドがない。データはHDDに1回だけしかwriteとreadが行われるだけである。


短所:
パーティションの数が大きくなると、膨大な出力ファイルのせいでパフォーマンスが低下していく。
多くのファイルがファイル・システムに書かれると、IO skewが発生しランダムIOが起こる。ランダムIOは一般的にはシーケンシャルIOの100倍遅いと言われている。参考としてこちらのリンクを挙げます。 IO operation slowness at the scale of millions of files on a single filesystem.

TODO: link to IO operation

And of course, when data is written to files it is serialized and optionally compressed. When it is read, the process is opposite – it is uncompressed and deserialized. Important parameter on the fetch side is “spark.reducer.maxSizeInFlight“ (48MB by default), which determines the amount of data requested from the remote executors by each reducer. This size is split equally by 5 parallel requests from different executors to speed up the process. If you would increase this size, your reducers would request the data from “map” task outputs in bigger chunks, which would improve performance, but also increase memory usage by “reducer” processes.

そしてもちろん、データがファイルに書かれるとき、データはシリアライズされ、オプションとして圧縮されます。データを読むときは、逆のことが置きます。解凍されデシリアライズされます。このfetchするときに重要なパラメータは、"spark.recuder.maxSizeInFlight"（デフォルトで48MB）それぞれのreducerによってリモートプロセスからアクセスされるデータの量を決定するものです。このサイズは処理の高速化のために違うエグゼキューターから5個の並列リクエストによって等しく分割されます。もしこのサイズを増加させると、reducerは"map"タスクからのデータを大きなチャンクでとりにいくでしょう。これはパフォーマンスを増加するかも知れませんが、reducerのプロセスのメモリ使用量を増加させます。

もしreduce側でリコードの順番が重要でないのなら、"reducer"は"map"の出力に依存したイテレータを返すだけです。しかし、もし順番が重要であれば、"reduce"側でExternalSorterを使いすべてのデータを取得し、ソートします。


Sort Shuffle

Starting Spark 1.2.0, this is the default shuffle algorithm used by Spark (spark.shuffle.manager = sort). In general, this is an attempt to implement the shuffle logic similar to the one used by Hadoop MapReduce. With hash shuffle you output one separate file for each of the “reducers”, while with sort shuffle you’re doing a smarted thing: you output a single file ordered by “reducer” id and indexed, this way you can easily fetch the chunk of the data related to “reducer x” by just getting information about the position of related data block in the file and doing a single fseek before fread. But of course for small amount of “reducers” it is obvious that hashing to separate files would work faster than sorting, so the sort shuffle has a “fallback” plan: when the amount of “reducers” is smaller than “spark.shuffle.sort.bypassMergeThreshold” (200 by default) we use the “fallback” plan with hashing the data to separate files and then joining these files together in a single file. This logic is implemented in a separate class BypassMergeSortShuffleWriter.

Spark 1.2.0からSparkのshuffleのアルゴリズムはsortがデフォルトで使われています。(spark.shuffle.manager = sort)　一般的には、これは、Hadoop MapReduceで使われているロジックと似たようなShuffleのロジックを実装したものです。Hash shuffleではそれぞれの"reducer"のために別々のファイルを出力しましたが、一方Sort shuffleはもっと賢い方法で行います。"reducer"のidでインデックス化された一つのファイルを出力します。この方法により、"reducer x"に紐付いたデータの塊はファイルの中のデータブロックの位置の情報を得し、そしてfreadの前にfseekを一回だけ行うことにデータを取得することができます。しかしながら、もちろん少ない数の”reducer”ではhashでファイルを分けたほうがsortより速く処理をします。なので、sort shuffleは"reducer"が”spark.shuffle.sort.bypassMergeThreshold”(デフォルトで200)より少なければ、hashをファイル分割するために使いそれらのファイルを一つのファイルにまとめるという"fallback" planを持っています。このロジックはBypassMergeSortShuffleWriterというクラスに分けて実装されています。


The funny thing about this implementation is that it sorts the data on the “map” side, but does not merge the results of this sort on “reduce” side – in case the ordering of data is needed it just re-sorts the data. Cloudera has put itself in a fun position with this idea: http://blog.cloudera.com/blog/2015/01/improving-sort-performance-in-apache-spark-its-a-double/. They started a process of implementing the logic that takes advantage of pre-sorted outputs of “mappers” to merge them together on the “reduce” side instead of resorting. As you might know, sorting in Spark on reduce side is done using TimSort, and this is a wonderful sorting algorithm which in fact by itself takes advantage of pre-sorted inputs (by calculating minruns and then merging them together). A bit of math here, you can skip if you’d like to. Complexity of merging M sorted arrays of N elements each is O(MNlogM) when we use the most efficient way to do it, using Min Heap. With TimSort, we make a pass through the data to find MinRuns and then merge them together pair-by-pair. It is obvious that it would identify M MinRuns. First M/2 merges would result in M/2 sorted groups, next M/4 merges would give M/4 sorted groups and so on, so its quite straightforward that the complexity of all these merges would be O(MNlogM) in the very end. Same complexity as the direct merge! The difference here is only in constants, and constants depend on implementation. So the patch by Cloudera engineers has been pending on its approval for already one year, and unlikely it would be approved without the push from Cloudera management, because performance impact of this thing is very minimal or even none, you can see this in JIRA ticket discussion. Maybe they would workaround it by introducing separate shuffle implementation instead of “improving” the main one, we’ll see this soon.

Fine with this. What if you don’t have enough memory to store the whole “map” output? You might need to spill intermediate data to the disk. Parameter spark.shuffle.spill is responsible for enabling/disabling spilling, and by default spilling is enabled. If you would disable it and there is not enough memory to store the “map” output, you would simply get OOM error, so be careful with this.

The amount of memory that can be used for storing “map” outputs before spilling them to disk is “JVM Heap Size” * spark.shuffle.memoryFraction * spark.shuffle.safetyFraction, with default values it is “JVM Heap Size” * 0.2 * 0.8 = “JVM Heap Size” * 0.16. Be aware that if you run many threads within the same executor (setting the ratio of spark.executor.cores / spark.task.cpus to more than 1), average memory available for storing “map” output for each task would be “JVM Heap Size” * spark.shuffle.memoryFraction * spark.shuffle.safetyFraction / spark.executor.cores * spark.task.cpus, for 2 cores with other defaults it would give 0.08 * “JVM Heap Size”.

Spark internally uses AppendOnlyMap structure to store the “map” output data in memory. Interestingly, Spark uses their own Scala implementation of hash table that uses open hashing and stores both keys and values in the same array using quadratic probing. As a hash function they use murmur3_32 from Google Guava library, which is MurmurHash3.

This hash table allows Spark to apply “combiner” logic in place on this table – each new value added for existing key is getting through “combine” logic with existing value, and the output of “combine” is stored as the new value.

When the spilling occurs, it just calls “sorter” on top of the data stored in this AppendOnlyMap, which executes TimSort on top of it, and this data is getting written to disk.

Sorted output is written to the disk when the spilling occurs or when there is no more mapper output, i.e. the data is guaranteed to hit the disk. Whether it will really hit the disk depends on OS settings like file buffer cache, but it is up to OS to decide, Spark just sends it “write” instructions.

Each spill file is written to the disk separately, their merging is performed only when the data is requested by “reducer” and the merging is real-time, i.e. it does not call somewhat “on-disk merger” like it happens in Hadoop MapReduce, it just dynamically collects the data from a number of separate spill files and merges them together using Min Heap implemented by Java PriorityQueue class.

This is how it works:

spark_sort_shuffle

So regarding this shuffle:

Pros:

Smaller amount of files created on “map” side
Smaller amount of random IO operations, mostly sequential writes and reads
Cons:

Sorting is slower than hashing. It might worth tuning the bypassMergeThreshold parameter for your own cluster to find a sweet spot, but in general for most of the clusters it is even too high with its default
In case you use SSD drives for the temporary data of Spark shuffles, hash shuffle might work better for you
Unsafe Shuffle or Tungsten Sort

Can be enabled with setting spark.shuffle.manager = tungsten-sort in Spark 1.4.0+. This code is the part of project “Tungsten”. The idea is described here, and it is pretty interesting. The optimizations implemented in this shuffle are:

Operate directly on serialized binary data without the need to deserialize it. It uses unsafe (sun.misc.Unsafe) memory copy functions to directly copy the data itself, which works fine for serialized data as in fact it is just a byte array
Uses special cache-efficient sorter UnsafeShuffleExternalSorter that sorts arrays of compressed record pointers and partition ids. By using only 8 bytes of space per record in the sorting array, it works more efficienly with CPU cache
As the records are not deserialized, spilling of the serialized data is performed directly (no deserialize-compare-serialize-spill logic)
    Extra spill-merging optimizations are automatically applied when the shuffle compression codec supports concatenation of serialized streams (i.e. to merge separate spilled outputs just concatenate them). This is currently supported by Spark’s LZF serializer, and only if fast merging is enabled by parameter “shuffle.unsafe.fastMergeEnabled”
    As a next step of optimization, this algorithm would also introduce off-heap storage buffer.

    This shuffle implementation would be used only when all of the following conditions hold:

    The shuffle dependency specifies no aggregation. Applying aggregation means the need to store deserialized value to be able to aggregate new incoming values to it. This way you lose the main advantage of this shuffle with its operations on serialized data
    The shuffle serializer supports relocation of serialized values (this is currently supported by KryoSerializer and Spark SQL’s custom serializer)
    The shuffle produces less than 16777216 output partitions
    No individual record is larger than 128 MB in serialized form
    Also you must understand that at the moment sorting with this shuffle is performed only by partition id, it means that the optimization with merging pre-sorted data on “reduce” side and taking advantage of pre-sorted data by TimSort on “reduce” side is no longer possible. Sorting in this operation is performed based on the 8-byte values, each value encodes both link to the serialized data item and the partition number, here is how we get a limitation of 1.6b output partitions.

    Here’s how it looks like:

    spark_tungsten_sort_shuffleFirst for each spill of the data it sorts the described pointer array and outputs an indexed partition file, then it merges these partition files together into a single indexed output file.

    Pros:

    Many performance optimizations described above
    Cons:

    Not yet handling data ordering on mapper side
    Not yet offer off-heap sorting buffer
    Not yet stable
    But in my opinion this sort is a big advancement in the Spark design and I would like to see how this will turn out and what new performance benchmarks Databricks team would offer us to show how cool the performance because with these new features.

    This is all what I wanted to say about Spark shuffles. It is a very interesting piece of the code and if you have some time I’d recommend you to read it by yourself.
