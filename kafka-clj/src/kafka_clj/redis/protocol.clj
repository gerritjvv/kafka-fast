(ns kafka-clj.redis.protocol
  (:refer-clojure :exclude [get set]))


;;;
;;;Protocol that the redis of kafka-clj uses to make single and cluster redis usage transparent
;;;Also two apis are used because carmine as of writing does not yet support redis cluster
;;;So we need to use Jedis and mimick carmine usage via Nippy
(defprotocol IRedis
  (-lpush [_ queue obj])
  (-llen  [_ queue])
  (-lrem  [_ queue n obj])
  (-get   [_ k])
  (-set   [_ k v])
  (-lrange [_ q n limit])
  (-brpoplpush [_ queue queue2 n])
  (-acquire-lock [_ lock-name timeout-ms wait-ms])
  (-release-lock [_ lock-name owner-uuid])
  (-have-lock?   [_ lock-name owner-uuid])
  (-flushall [_])
  (-close! [_])
  (-wcar [_ f]))