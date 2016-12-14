{-
TODO:
- [ ] Persistence of reports
- [ ] Persistence of per-5-min results
- [ ] Data locality
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fdefer-type-errors #-}

module Main where

import Control.Concurrent (threadDelay)
import Control.DeepSeq
import qualified Control.Foldl as L
import Control.Monad (when)
import Control.Monad.IO.Class
import Control.Monad.Trans.Class (lift)
import Data.Aeson (ToJSON, FromJSON)
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import Data.Foldable
import Data.HashMap.Monoidal (MonoidalHashMap)
import qualified Data.HashMap.Monoidal as M
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as H
import qualified Data.HashSet as HS
import Data.Hashable
import Data.Hashable.Time ()
import Data.Maybe
import Data.Monoid
import Data.Proxy
import qualified Data.String.Class as S
import Data.Text (Text)
import Data.Time
import Data.Time.Clock.POSIX
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import GHC.Generics
import qualified Network.Riak as R
import qualified Network.Riak.Cluster as R
import qualified Network.Riak.Types as R
import Pipes ((>->))
import qualified Pipes as P
import qualified Pipes.Prelude as P

-- * General datatypes
-- | Datatype describing a living person
data Person = Person
  { pName :: Text
  , pBirthdy :: Day
  } deriving (Eq, Show, Generic)

instance ToJSON Person where
  toJSON = J.genericToJSON (toJsonOpts 1)

instance FromJSON Person where
  parseJSON = J.genericParseJSON (toJsonOpts 1)

instance Hashable Person

instance NFData Person

-- * Event types
-- | Event representing person's confusion
data ConfusionEvent = ConfusionEvent
  { ceReason :: Text
  , ceLevel :: Percentage
  , ceTimestamp :: UTCTime
  } deriving (Eq, Show, Generic)

instance Hashable ConfusionEvent

instance NFData ConfusionEvent

instance ToJSON ConfusionEvent where
  toJSON = J.genericToJSON (toJsonOpts 2)

instance FromJSON ConfusionEvent where
  parseJSON = J.genericParseJSON (toJsonOpts 2)

-- | Event representing person's happiness
data HappinessEvent = HappinessEvent
  { heReason :: Text
  , heLevel :: Percentage
  , heTimestamp :: UTCTime
  } deriving (Eq, Show, Generic)

instance ToJSON HappinessEvent where
  toJSON = J.genericToJSON (toJsonOpts 2)

instance FromJSON HappinessEvent where
  parseJSON = J.genericParseJSON (toJsonOpts 2)

instance Hashable HappinessEvent

instance NFData HappinessEvent

-- * Event-storage
class (Eq e, Show e, Hashable e, ToJSON e, FromJSON e, NFData e) =>
      Event e  where
  eventBucketP :: Proxy e -> R.Bucket
  eventBucket :: e -> R.Bucket
  eventBucket _ = eventBucketP (Proxy :: Proxy e)
  eventTimestamp :: e -> UTCTime

instance Event ConfusionEvent where
  eventBucketP _ = "confusion-events"
  eventTimestamp = ceTimestamp

instance Event HappinessEvent where
  eventBucketP _ = "happiness-events"
  eventTimestamp = heTimestamp

-- * Helper data-structures
newtype Percentage =
  Percentage Int
  deriving (Show, Eq, Generic, ToJSON, FromJSON)

instance Hashable Percentage

instance NFData Percentage

newtype Batch ev =
  Batch [ev]
  deriving (Eq, Show, Monoid, ToJSON, FromJSON, NFData)

unBatch :: Batch ev -> [ev]
unBatch (Batch evs) = evs

instance Event ev =>
         R.Resolvable (Batch ev) where
  resolve (Batch as) (Batch bs) =
    Batch (HS.toList (HS.union (HS.fromList as) (HS.fromList bs)))

type StartTime = POSIXTime

type EndTime = POSIXTime

-- TODO: validate
mkPercentage :: Int -> Percentage
mkPercentage i = Percentage i

-- * Folds
-- | Count events via accepting `keyF` argument.
countFoldIntKeyF
  :: (Hashable k, Eq k)
  => (ev -> Maybe k) -> L.Fold ev (MonoidalHashMap k (Sum Int))
countFoldIntKeyF keyF = L.Fold step mempty id
  where
    step m ev = maybe m (\k -> M.modify (+ 1) k m) (keyF ev)

-- | Key/value version of a fold
countFoldIntKV
  :: (Hashable k, Eq k)
  => L.Fold (k, ev) (MonoidalHashMap k (Sum Int))
countFoldIntKV = L.Fold step mempty id
  where
    step m (k, _) = M.modify (+ 1) k m

-- * Main actions
cleanupDb :: R.Cluster -> IO ()
cleanupDb riak = do
  let buckets =
        [ eventBucketP (Proxy :: Proxy ConfusionEvent)
        , eventBucketP (Proxy :: Proxy HappinessEvent)
        ] :: [R.Bucket]
  forM_ buckets $ \b -> cleanUntilEmpty riak b
  where
    cleanUntilEmpty riak b = R.inCluster riak $ \c -> cleanUntilEmpty' riak b c
    cleanUntilEmpty' riak b c = do
      n <- R.foldKeys c Nothing b (delAndCount riak b) 0
      when (n > 0) $
        do print ("> " <> show n <> " more keys to delete from " <> show b)
           threadDelay 600000
           cleanUntilEmpty' riak b c
    delAndCount r b i k =
      R.inCluster r $
      \c -> do
        R.delete c Nothing b k R.Default
        return (i + 1)

fillDb :: R.Cluster -> IO ()
fillDb riak = do
  t <- getCurrentTime
  let happys =
        [ HappinessEvent "no reason" (mkPercentage 80) (addUTCTime (-5) t)
        , HappinessEvent "no reason" (mkPercentage 85) (addUTCTime (-4) t)
        ]
  storeEvents riak happys

storeEvents
  :: forall ev.
     Event ev
  => R.Cluster -> [ev] -> IO ()
storeEvents riak evs = do
  let batches :: HashMap POSIXTime (Batch ev)
      batches = H.fromListWith (<>) (map evToRoundedPosixPair evs)
  forM_ (H.toList batches) (storeBatch riak)
  where
    evToRoundedPosixPair ev =
      (roundBy5min (utcTimeToPOSIXSeconds (eventTimestamp ev)), Batch [ev])
    storeBatch :: R.Cluster -> (POSIXTime, Batch ev) -> IO ()
    storeBatch riak (pt, batch) = do
      _ <- R.inCluster riak $
        \c -> do
          key <- (S.fromString . UUID.toString) <$> UUID.nextRandom
          let ix = [R.IndexInt "min" (truncate pt)]
          let b = eventBucketP (Proxy :: Proxy ev)
          R.putIndexed c Nothing b key ix Nothing batch R.Default R.Default
      return ()

-- TODO: get rid of deepseqPipe?
produceFromBucket
  :: forall m ev.
     (MonadIO m, Event ev)
  => R.Cluster -> StartTime -> EndTime -> P.Producer ev m ()
produceFromBucket riak start end =
  ((lift (getKeys riak start (end - 1) bucket) >>= (P.each . splitN 10))) >->
  getKeyGroup riak bucket >->
  P.concat >->
  P.tee P.print >->
  deepseqPipe
  where
    bucket = eventBucketP (Proxy :: Proxy ev)

-- | Like `Pipes.Prelude.seq`, except it evaluates values flowing downstream to
-- NF instead of WHNF.
--
-- A candidate for `Pipes.Prelude.Additional`?
deepseqPipe
  :: forall a m r.
     (NFData a, Monad m)
  => P.Pipe a a m r
deepseqPipe = P.for P.cat $ \a -> P.yield $!! a

getKeyGroup
  :: forall ev m.
     (Event ev, MonadIO m)
  => R.Cluster -> R.Bucket -> P.Pipe [R.Key] [ev] m ()
getKeyGroup riak bucket =
  P.for P.cat $
  \keys -> do
    it <-
      lift . liftIO . (R.inCluster riak) $
      \c -> do
        values <- liftIO (R.getMany c Nothing bucket keys R.Default)
        let res = map (unBatch . fst) . catMaybes $ values
        return $ concat res
    P.yield it

-- | Get keys by timestamp range
getKeys
  :: MonadIO m
  => R.Cluster -> StartTime -> EndTime -> R.Bucket -> m [R.Key]
getKeys riak startTime endTime b =
  liftIO $
  R.inCluster riak $
  \c -> liftIO (R.getByIndex c b (R.IndexQueryRangeInt "min" si ei))
  where
    si = (fromIntegral (truncate startTime :: Int))
    ei = (fromIntegral (truncate endTime :: Int))

countHappinessEventsReport :: R.Cluster -> IO Int
countHappinessEventsReport riak = do
  t <- getCurrentTime
  let start = roundBy5min (utcTimeToPOSIXSeconds (addUTCTime (-1000) t))
  let end = roundBy5min (utcTimeToPOSIXSeconds (addUTCTime (5 * 60 - 100) t))
  let kf _ = Just ()
  let prod = produceFromBucket riak start end :: P.Producer HappinessEvent IO ()
  -- keyF-folding
  res <- L.purely P.fold (countFoldIntKeyF kf) prod
  print res
  -- KV-folding
  res' <- L.purely P.fold countFoldIntKV (prodWithKey kf prod)
  print res'
  return (maybe 0 (\(Sum x) -> x) (M.lookup () res))

prodWithKey
  :: Monad m
  => (ev -> Maybe k) -> P.Producer ev m () -> P.Producer (k, ev) m ()
prodWithKey kf prod = P.for prod f
  where
    f ev = do
      case kf ev of
        Just k -> P.yield (k, ev)
        Nothing -> return ()

main :: IO ()
main = do
  putStrLn "> connecting to riak"
  riak <- connectToRiak
  putStrLn "> cleaning up riak"
  cleanupDb riak
  putStrLn "> filling riak"
  fillDb riak
  putStrLn "> counting data"
  r <- countHappinessEventsReport riak
  putStrLn "> final result received:"
  print r

-- * Helpers
connectToRiak :: IO R.Cluster
connectToRiak = do
  R.connectToCluster [R.defaultClient]

toJsonOpts :: Int -> J.Options
toJsonOpts n =
  J.defaultOptions
  { J.fieldLabelModifier = J.camelTo2 '_' . drop n
  }

roundBy5min :: POSIXTime -> POSIXTime
roundBy5min pt = fromIntegral ((truncate pt :: Int) `div` (5 * 60) * (5 * 60))

splitN :: Int -> [a] -> [[a]]
splitN _ [] = []
splitN 0 xs = [xs]
splitN n xs = (take n xs) : splitN n (drop n xs)
