[翻訳] Spark Architecture: Shuffle


TungstenのShuffleまでカバーされていて、感動してしまったので、翻訳（意訳）してみました。この人の他の記事もとても詳しくわかりやすく書いてあるのでぜひ原文で読んでみてください。自分の勉強もかね逐次翻訳はしていきます。

元記事: [Spark Architecture: Shuffle](http://0x0fff.com/spark-architecture-shuffle/)

# Spark Architecture: Shuffle

これは Apache Spark のアーキテクチャに関する2本目の記事です。今回は、Spark デザインの中でももっと面白い Shuffle について詳しく紹介していこうと思います。[前回](http://0x0fff.com/spark-architecture/)の記事は、Spark のアーキテクチャとメモリ管理についてでした。

![Spark-Shuffle-Design](http://0x0fff.com/wp-content/uploads/2015/08/Spark-Shuffle-Design.png)

一般的に Shuffle とはなんでしょうか？ 電話の履歴のリストがテーブルにあり、それぞれの日にどのくらい電話があったかを数える場合を考えてください。この場合は、各レコードを"day"をキーにして（それぞれの電話の回数として）バリューを"1"として変換します。この処理の後、各キーごとのバリューを合計することでそれぞれの日の電話の回数が得られます。しかし、データがクラスターに分散して保存されている場合、どのように同じキーで違うマシンに保存されたバリューを足し合わせるでしょうか？これを行うたった一つの方法は、同じキーを持つデータを同じマシンに集めることです。この処理の後、データは足し合わせることができるようになるでしょう。

クラスターをまたがってデータを shuffle するには、様々なタスクが必要とされます。たとえば、"id"というフィールドで2つのテーブルをジョインするとき、同じidの値を持つすべてのデータが同じチャンクに保存されていないといけません。テーブルに1から1000000の範囲で整数のキーが存在するときを考えましょう。キーが1-100のデータがパーティションとして一つのチャンクに保存されていると仮定しましょう。そして、同じチャンクのデータをソートすることによってパーティション同士を直接ジョインすることができます。なぜならキーが1-100のデータはこれらの2つのパーティションに保存されていることがわかっているからです。これを実現されるには、2つのテーブルに同じだけのパーティションがなければなりません。この方法によって、ジョインは計算コストが少なくできます。さて、これで Shuffle の重要性がわかったと思います。

このトピックを議論するにあたり、MapReduce の命名規則に従い話します。suffle 実行する中では、source executor として、データを放出するものを"mapper"と呼び。そのデータを消費する target executor を"reducer"と呼びます。これらの mapper と reducer の間で行われるのが"shuffle"です。

Shuffle は一般的に2つの重要なパラメータを取ります。 ***spark.shuffle.compress*** shuffleのアウトプットを圧縮するかどうか。 ***spark.shuffle.spill.compress*** Shuffleでメモリからあふれた中間ファイルを圧縮するかどうか。両者ともデフォルトで"true"になっています。 ***spark.io.compression.codec*** を両者とも使っています。データを圧縮するためのcodecはデフォルトでは[snappy](https://en.wikipedia.org/wiki/Snappy_(software))です。

ご存知かも知れませんが、Spark には数種類の Shuffle の実装がされています。 ***spark.shuffle.manager***のパラメータによってそれらのShuffleを使うことができます。使えるShuffleの実装はhash, sort, tungsten-sortです。1.2 からのデフォルトは"sort"になっています。

## Hash Shuffle


Spark 1.2.0より前は、hash がデフォルトでした。しかし、hashは多くの欠点がありました。それらは、[生成されるファイルの多さに起因するものです。](http://www.cs.berkeley.edu/~kubitron/courses/cs262a-F13/projects/reports/project16_report.pdf)それぞれの mapper タスクがそれぞれのreducerのために作る分割ファイルの量は、クラスターのなかの **M * R** だけの数になります。（ **M** は mapper の数、 **R** は reducer の数です。）多くの mapper と reducer は大きな問題を生じます。mapper と reducer の buffer サイズ、ファイルシステムの中でオープンされているファイルの数、それらのファイルを作成、削除する速さの問題などです。[いい例としては Yahoo が実際にこの問題に直面した時の例です。](http://spark-summit.org/2013/wp-content/uploads/2013/10/Li-AEX-Spark-yahoo.pdf)彼らは46kのmapperと46kのreducerを使い20億のファイルをクラスターの中で作成しました。

この Shuffle の仕組みは賢いものではありません。reduce 側でreducer の数をパーティションの数として数え、それらに対して別々のファイルを作成しアウトプットが必要なレコードをループします。target partiitionをそれぞれのreducerのために数え、対応したファイルに結果を書き出します。

このように動きます。
![spark_hash_shuffle_no_consolidation](http://0x0fff.com/wp-content/uploads/2015/08/spark_hash_shuffle_no_consolidation.png)

このshuffleには最適化した実装があります。それは、 ***spark.shuffle.consolidateFiles*** というパラメータで管理することができます。(デフォルトでは "false")　それが "true" になった時、"mapper" の出力ファイルは統合されます。もしクラスターが *E* 個のエグゼキューター( YARN では ***-num-executors***) とそれらが持つ *C* 個のコア(***spark.executor.cores*** か YARNでは"-executor-cores")と、それぞれのタスクのための **T** 個CPU( ***spark.task.cpus***) とするとクラスターの実行されるスロットの数は **E * C / T** になります。そして Shuffle の間に作られるファイルは **E * C / T * R** となります。10個のコアと100個のエグゼキューターで、それぞれが1コアあたりそれぞれのタスクに割り当てると46000の"reducers"では20億のファイルから4600万のファイルまで減らすことができます。これはパフォーマンス的にはかなりよい結果となります。この機能は新しくファイルを reducer のために作るのではなく、作られたファイルをプールすることによって実装されています。この処理が終わると、 **R** 個のファイルグループをプールに返します。それぞれのエグゼキューターでは **C / T** 個のタスクが並列で実行されます。それらは各グループには **R** 個のファイルがある **C / T** 個のグループのを作ります。 最初の **C / T** の"map"処理が終わったあと次の"map"タスクが行われ、すでにあるグループのファイルを再利用します。

このように動きます。
![spark_hash_shuffle_no_consolidation](http://0x0fff.com/wp-content/uploads/2015/08/spark_hash_shuffle_with_consolidation.png)


長所:

- 高速である。ソートは必要ない。ハッシュテーブルを維持する必要が無い
- ソートするためのメモリオーバーヘッドがない。
- IOのオーバーヘッドがない。データはHDDに1回だけしかwriteとreadが行われるだけである。


短所:

- パーティションの数が大きくなると、膨大な出力ファイルのせいでパフォーマンスが低下していく。
- 多くのファイルがファイル・システムに書かれると、IO skewが発生しランダムIOが起こる。ランダムIOは一般的にはシーケンシャルIOの100倍遅いと言われている。参考としてこちらのリンクを挙げます。 

IOの遅さの参考にとして　[One Billion Files](http://events.linuxfoundation.org/slides/2010/linuxcon2010_wheeler.pdf)


そしてもちろん、データがファイルに書かれるとき、データはシリアライズされ、オプションとして圧縮されるかもしれません。データを読むときは、逆のことが起きます。解凍されデシリアライズされます。この fetch するときに重要なパラメータは、 ***spark.recuder.maxSizeInFlight***（デフォルト: 48MB）それぞれのreducerによってリモートプロセスからアクセスされるデータの量を決定するパラメータです。処理の高速化のために違うエグゼキューターから5個の並列リクエストによってこのサイズが等しく分割されます。もしこのサイズを増加させると、 reducer は "map" タスクからのデータを大きなチャンクでとりにいくでしょう。これはパフォーマンスを増加するかも知れませんが、reducer のプロセスのメモリ使用量を増加させます。

もしreduce側でリコードの順番が重要でないのなら、"reducer"は"map"の出力に依存したイテレータを返すだけです。しかし、もし順番が重要であれば、"reduce"側でExternalSorterを使いすべてのデータを取得し、ソートします。


## Sort Shuffle

Starting Spark 1.2.0, this is the default shuffle algorithm used by Spark (spark.shuffle.manager = sort). In general, this is an attempt to implement the shuffle logic similar to the one used by Hadoop MapReduce. With hash shuffle you output one separate file for each of the “reducers”, while with sort shuffle you’re doing a smarted thing: you output a single file ordered by “reducer” id and indexed, this way you can easily fetch the chunk of the data related to “reducer x” by just getting information about the position of related data block in the file and doing a single fseek before fread. But of course for small amount of “reducers” it is obvious that hashing to separate files would work faster than sorting, so the sort shuffle has a “fallback” plan: when the amount of “reducers” is smaller than “spark.shuffle.sort.bypassMergeThreshold” (200 by default) we use the “fallback” plan with hashing the data to separate files and then joining these files together in a single file. This logic is implemented in a separate class BypassMergeSortShuffleWriter.

Spark 1.2.0から Spark の Shuffle のアルゴリズムはsortがデフォルトで使われています。( ***spark.shuffle.manager*** = sort)　一般的には、これは、Hadoop MapReduceで使われているロジックと似たようなShuffleのロジックを実装したものです。Hash shuffleではそれぞれの"reducer"のために別々のファイルを出力しましたが、一方Sort shuffleはもっと賢い方法で行います。"reducer"のidでインデックス化された一つのファイルを出力します。この方法により、"reducer x"に紐付いたデータの塊はファイルの中のデータブロックの位置の情報を得し、そしてfreadの前にfseekを一回だけ行うことにデータを取得することができます。しかしながら、もちろん少ない数の”reducer”ではhashでファイルを分けたほうがsortより速く処理をします。なので、sort shuffleは"reducer"が”spark.shuffle.sort.bypassMergeThreshold”(デフォルトで200)より少なければ、hashをファイル分割するために使いそれらのファイルを一つのファイルにまとめるという"fallback" planを持っています。このロジックはBypassMergeSortShuffleWriterというクラスに分けて実装されています。


The funny thing about this implementation is that it sorts the data on the “map” side, but does not merge the results of this sort on “reduce” side – in case the ordering of data is needed it just re-sorts the data. Cloudera has put itself in a fun position with this idea: http://blog.cloudera.com/blog/2015/01/improving-sort-performance-in-apache-spark-its-a-double/. They started a process of implementing the logic that takes advantage of pre-sorted outputs of “mappers” to merge them together on the “reduce” side instead of resorting. As you might know, sorting in Spark on reduce side is done using TimSort, and this is a wonderful sorting algorithm which in fact by itself takes advantage of pre-sorted inputs (by calculating minruns and then merging them together). A bit of math here, you can skip if you’d like to. Complexity of merging M sorted arrays of N elements each is O(MNlogM) when we use the most efficient way to do it, using Min Heap. With TimSort, we make a pass through the data to find MinRuns and then merge them together pair-by-pair. It is obvious that it would identify M MinRuns. First M/2 merges would result in M/2 sorted groups, next M/4 merges would give M/4 sorted groups and so on, so its quite straightforward that the complexity of all these merges would be O(MNlogM) in the very end. Same complexity as the direct merge! The difference here is only in constants, and constants depend on implementation. So the patch by Cloudera engineers has been pending on its approval for already one year, and unlikely it would be approved without the push from Cloudera management, because performance impact of this thing is very minimal or even none, you can see this in JIRA ticket discussion. Maybe they would workaround it by introducing separate shuffle implementation instead of “improving” the main one, we’ll see this soon.

この実装で興味深いのは、"map"側でデータをソートし、"reduce"側では、このソートの結果をマージしないところにあります。もし、データの順番が必要であれば、再びソートしなおします。Clouderaはこの興味深いところに対する意見をここで述べています。http://blog.cloudera.com/blog/2015/01/improving-sort-performance-in-apache-spark-its-a-double/ 彼らは、再びソートする代わりに予めソートしてある"mapper"の出力を"reduce"側でマージするために有効活用するロジックを考え始めました。ご存知かもしれませんが、Sparkのreduce側でのソートはTimSortを使っています。minrunsを計算し、それらをマージしていくことによって予めソートしてある入力に対しては有用性がある素晴らしいソートアルゴリズムです。すこし数学の話です。これは読み飛ばしても構いません。N個の要素のM個のソート済みアレイをマージする複雑さはもっと恋率のいいMin Heapを使った場合O(MNlogM)です。TimSortを使うとMinRunsを探すためにデータをpass throughしてそれらの組同士をマージしていきます。今回の場合は明らかにM MinRunsです。最初のM/2のマージはM/2ソート済みのグループになります。次のM/4のマージはM/4のソート済みのグループになります。その次の場合も同様です。このようにすべてのマージが最終的にO(MNlogM)になるのは簡単に理解できます。ここでの違いは一定さだけです。一定になるかどうかは実装によります。なのでCloudera エンジニアのパッチは1年以上も承認がとれずにいます。しかし、これはCloudera managementからの圧力がなくともマージされるでしょう、この実装パフォーマンスへの影響は、とても少ないかないほどです。JIRAのチケットの議論を見てみるといいでしょう。彼らはshuffleの改善ではなく、別の実装として導入しようと試みるでしょう。

この方法でいいとします。では、"map"全体の出力に対して保存するだけのメモリがなかった場合はどうするのでしょうか？中間ファイルとしてディスクに書き込まないといけなくなるでしょう。spark.shuffle.spillというパラメータによって有効にするかどうかが決まられます。デフォルトではディスクへの書き込みは有効になっています。もしも無効にしてしまった場合、"map"の出力がメモリに書ききれない場合はOOM エラーになる可能性があるので気をつけてください。

The amount of memory that can be used for storing “map” outputs before spilling them to disk is “JVM Heap Size” * spark.shuffle.memoryFraction * spark.shuffle.safetyFraction, with default values it is “JVM Heap Size” * 0.2 * 0.8 = “JVM Heap Size” * 0.16. Be aware that if you run many threads within the same executor (setting the ratio of spark.executor.cores / spark.task.cpus to more than 1), average memory available for storing “map” output for each task would be “JVM Heap Size” * spark.shuffle.memoryFraction * spark.shuffle.safetyFraction / spark.executor.cores * spark.task.cpus, for 2 cores with other defaults it would give 0.08 * “JVM Heap Size”.

"map"がデータをディスクに書き込む前に使えるメモリの容量は "JVM Heap Size" * "spark.shuffle.memoryFraction" * "spark.shuffle.safetyFraction"です。デフォルトでは、"JVM Heap Size" * 0.2 * 0.8 = "JVM Heap Size" * 0.16です。同じエグゼキュータの中で多くのスレッドを上げる場合は気をつけてください。（spark.executor.cores / spark.task.cpus の割合が1以上になった場合）それぞれのタスクの"map"の出力を保存するのに必要なメモリの平均は “JVM Heap Size” * spark.shuffle.memoryFraction * spark.shuffle.safetyFraction / spark.executor.cores * spark.task.cpus になるでしょう。2コアでパラメータがでファオルトの値であれば、0.08 * "JVM Heap Size"になるでしょう。

Spark内部では、AppendOnlyMapの構造を"map"の出力をメモリに保存するために使っています。興味深いことにSparkは自前のScala実装のHashテーブルを使用しています。それopen hashingを使っていてquadratic probingを使いキーとバリューを同じアレイに保存しています。hashとしてはMurmurHash3の実装であるGoogle Guava libraryの murmur3_32を使っています。

This hash table allows Spark to apply “combiner” logic in place on this table – each new value added for existing key is getting through “combine” logic with existing value, and the output of “combine” is stored as the new value.
このhash テーブルはSparkに"combiner"のロジックをhashテーブルで実行することを可能にします。既存のキーに対して新しいバリューが追加されると既存のバリューと合わせてcombineのロジックが適用されます。そして"combine"の出力が新しい値として保存されます。

データがメモリからあふれた時、Hashテーブルは、TimSortをこのAppendOnlyMapに保存されたデータに対して実行する"sorter"を呼びデータをディスクに書き込みます。

Sorted output is written to the disk when the spilling occurs or when there is no more mapper output, i.e. the data is guaranteed to hit the disk. Whether it will really hit the disk depends on OS settings like file buffer cache, but it is up to OS to decide, Spark just sends it “write” instructions.

データがメモリからあふれた時もしくは、mapperの出力がなくなった時、ソートされた出力はディスクに書き込まれます。たとえば、データがディスクに書き込まれることが保証されている場合です。データが書き込まれるかどうかは、ファイルバッファキャシュのようなOSの設定によります。Sparkは単純にwriteの命令を発行するだけなのでOSによります。


Each spill file is written to the disk separately, their merging is performed only when the data is requested by “reducer” and the merging is real-time, i.e. it does not call somewhat “on-disk merger” like it happens in Hadoop MapReduce, it just dynamically collects the data from a number of separate spill files and merges them together using Min Heap implemented by Java PriorityQueue class.

あふれたファイルはディスクに分けてそれぞれ書きだされます。このファイルのマージは"reducer"がデータを要求した時にリアルタイにマージされていきます。たとえば、Hadoop MapReduceで行われている"on-disk merger"を呼ぶわけではなく、分割されたメモリからあふれたファイルを動的にあつめ、Java PriorityQueue classで実装されているMin Heapを使いそれらをマージしていきます。


This is how it works:

![spark_sort_shuffle](http://0x0fff.com/wp-content/uploads/2015/08/spark_sort_shuffle-1024x459.png)

Sort Shuffleに関して

長所:

- "map"側では少ない量のファイルが作られる。
random IOが少なくなりシーケンシャルなreadとwriteを使う。

短所:

- Sortはhashよりも低速である。一般的には、デフォルトの設定で十分であるが、bypassMergeThresholdのパラメータをあなたのクラスタのsweet spotを見つけるのにはチューニングする価値があるかもしれない。

- SSD ドライブをSparkShuffleのテンポラリデータのストレージにつかっているならhash shuffleのほうが良い性能がでるかもしれない。

Unsafe Shuffle or Tungsten Sort

TODO: insert link
Sparkの1.4.0以上では、spark.suffle.manager = tungsten-sortが有効にされているかもれない。このコードは"Tungsten" プロジェクトからポートされたものである。このアイディアはここで説明されている。それはとても興味深いものです。この最適化されたShuffleの実装の特徴は下記のとおりです。


Operate directly on serialized binary data without the need to deserialize it. It uses unsafe (sun.misc.Unsafe) memory copy functions to directly copy the data itself, which works fine for serialized data as in fact it is just a byte array
データをデシリアライズせずにシリアライズされたバイナリデータをそのまま扱う。これはunsafe(sun.misc.Unsafe) memoruy copy関数を直接データをコピーするために使用している。この関数は、シリアライズされたデータ(実際は単なるbyte array)に対して有効に働く。

Uses special cache-efficient sorter UnsafeShuffleExternalSorter that sorts arrays of compressed record pointers and partition ids. By using only 8 bytes of space per record in the sorting array, it works more efficienly with CPU cache
パーティション idと圧縮されたarrayのレコードのポインタをソートするキャッシュを有効に使う UnsafeShuffleExternalSorterという特殊なsorterを使っている。たった8byteの領域を1レコードあたりにソートされたアレイでつか言うことにより、CPUキャッシュがより効率良く働きます。

As the records are not deserialized, spilling of the serialized data is performed directly (no deserialize-compare-serialize-spill logic)

レコードはでシリアライスされていないので、メモリからあふれたシリアライズデータは直接処理されます。(deserialize-compare-serialize-spillのロジックは適応されることはありません)

    Extra spill-merging optimizations are automatically applied when the shuffle compression codec supports concatenation of serialized streams (i.e. to merge separate spilled outputs just concatenate them). This is currently supported by Spark’s LZF serializer, and only if fast merging is enabled by parameter “shuffle.unsafe.fastMergeEnabled”

その他のメモリからあふれたデータのマージに対しての最適化はshuffleの圧縮codecがシリアライズストリームの結合をサポートするときに自動的に適用されます。

最適化の次のステップとして、このアルゴリズムははoff-heapストレージのバッファを導入します。

    This shuffle implementation would be used only when all of the following conditions hold:
このShuffleの実装は、下記の条件が満たされた時のみ実行されます。

- The shuffle dependency specifies no aggregation. Applying aggregation means the need to store deserialized value to be able to aggregate new incoming values to it. This way you lose the main advantage of this shuffle with its operations on serialized data
 Shuffleの依存関係に集約がない。集約を行うということは、デシリアライズされた値をincoming valueとしてデシリアライズされたデータに集約しないといけない。
- Shuffleのシリアライザがシリアライズされたバリューの関係性をサポートしていること。（これはKryoSerializertoSpark SQLのカスタムシリアライザでサポートされています。）
- Shuffleが16777216より少ないパーティションの出力を生成すること
- すべてのレコードが128MBを超えないシリアライズされたデータであること

現在のこのShuffleの実装はパーティション idにのみ実行されており、"reduce"側でのソート済みデータをマージする最適化とTimSortによって得られるソート済みのデータをソートするを"reduce"側での最適化は使えなくなっております。この処理のソートでは、それぞれのバリューがシリアライズされたデータをパーティションの番号の両方に紐付いている8byteの値をつかって行われているため、約16億のパーティションしか扱うことができません。

Here’s how it looks like:


![spark_tungsten_sort](http://0x0fff.com/wp-content/uploads/2015/08/spark_tungsten_sort_shuffle.png)


First for each spill of the data it sorts the described pointer array and outputs an indexed partition file, then it merges these partition files together into a single indexed output file.

長所:

- 上記で述べたようなパフォーマンスへの最適化がされている。

短所:

- まだmapper側でオーダリングされたデータを扱えない.
- まだoff heapのソーティングバファを提供していない
- まだ安定していない。

しかし、個人的な意見として、このソートはSparkのデザインにとってとても大きな成果です。私は、Databricks　チームがこの新しい特徴によって得られたパフォーマンスの素晴らしさを見せるために提供する新しいベンチマークがどうなるか、今後どうなっていくのかが見てみたいです。

これが私のSparkのShuffleについて言いたかったことの全てです。とても興味深いコードでした。もしもあなたが時間があるのなら、自分でコードを読んで見ることをおすすめします。

