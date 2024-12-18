---
slug: /ja/faq/general/mapreduce
title: MapReduceのようなものを使わない理由
toc_hidden: true
toc_priority: 110
---

# MapReduceのようなものを使わない理由 {#why-not-use-something-like-mapreduce}

MapReduceのようなシステムは、分散ソートに基づくreduce操作を行う分散コンピューティングシステムとして参照できます。このクラスで最も一般的なオープンソースソリューションは、[Apache Hadoop](http://hadoop.apache.org)です。

これらのシステムは高レイテンシーのため、オンラインクエリには適していません。言い換えれば、ウェブインターフェースのバックエンドとして使用することはできません。リアルタイムデータの更新にも不向きです。分散ソートは、操作結果やすべての中間結果（存在する場合）が単一サーバーのRAMにあるのが通常のオンラインクエリでは、reduce操作を実行する最善の方法ではありません。このような場合、ハッシュテーブルがreduce操作を行うための最適な方法です。map-reduceタスクを最適化する一般的なアプローチは、RAM内でハッシュテーブルを使用した事前集計（部分的なreduce）です。この最適化はユーザーが手動で行います。分散ソートは、簡単なmap-reduceタスクを実行する際のパフォーマンス低下の主な原因の一つです。

ほとんどのMapReduceの実装は、クラスター上で任意のコードを実行することを可能にしますが、実験を迅速に実行するためにはOLAPに適した宣言型クエリ言語の方が優れています。例えば、HadoopにはHiveやPigがあります。また、Cloudera ImpalaやShark（廃止された）をSpark用として考慮することもできますし、Spark SQL、Presto、Apache Drillも同様です。このようなタスクを実行する際のパフォーマンスは、専門システムと比較して非常に非効率的ですが、比較的高いレイテンシーにより、これらのシステムをウェブインターフェースのバックエンドとして使用することは現実的ではありません。
