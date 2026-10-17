package com.interana.eventsim.config

import com.interana.eventsim.{Constants, State, WeightedRandomThingGenerator}

import scala.collection.mutable
import scala.io.Source
import ujson._
import upickle.core.LinkedHashMap

/**
 *  Site configuration (loaded from JSON file, used to run simulation)
 */

object ConfigFromFile {

  val initialStates = scala.collection.mutable.HashMap[(String,String),WeightedRandomThingGenerator[State]]()

  new State(("NEW_SESSION","INITIAL_STATUS",200,"",""))
  val showUserWithState = new mutable.HashMap[String, Boolean]()
  val levelGenerator = new WeightedRandomThingGenerator[String]()
  val authGenerator = new WeightedRandomThingGenerator[String]()

  // optional config values
  var alpha:Double = 60.0
  var beta:Double  = Constants.SECONDS_PER_DAY * 3

  var damping:Double = Constants.DEFAULT_DAMPING
  var weekendDamping:Double = Constants.DEFAULT_WEEKEND_DAMPING
  var weekendDampingOffset:Int = Constants.DEFAULT_WEEKEND_DAMPING_OFFSET
  var weekendDampingScale:Int = Constants.DEFAULT_WEEKEND_DAMPING_SCALE
  var sessionGap:Int = Constants.DEFAULT_SESSION_GAP
  var churnedState:Option[String] = None
  var seed = 0L
  var newUserAuth:String = Constants.DEFAULT_NEW_USER_AUTH
  var newUserLevel:String = Constants.DEFAULT_NEW_USER_LEVEL

  var startDate:Option[String] = None
  var endDate:Option[String] = None
  var nUsers:Option[Int] = None
  var firstUserId:Option[Int] = None
  var growthRate:Option[Double] = None
  var tag:Option[String] = None

  // tags for JSON config file
  val TRANSITIONS = "transitions"
  val NEW_SESSION = "new-session"
  val NEW_USER_AUTH = "new-user-auth"
  val NEW_USER_LEVEL = "new-user-level"
  val CHURNED_STATE = "churned-state"
  val SHOW_USER_DETAILS = "show-user-details"
  val PAGE = "page"
  val AUTH = "auth"
  val SHOW = "show"
  val SOURCE = "source"
  val DEST = "dest"
  val P = "p"
  val STATUS = "status"
  val METHOD = "method"
  val LEVEL = "level"
  val LEVELS = "levels"
  val AUTHS = "auths"
  val WEIGHT = "weight"
  val SEED = "seed"
  val SESSION_GAP = "session-gap"

  val ALPHA = "alpha"
  val BETA = "beta"
  val DAMPING = "damping"
  val WEEKEND_DAMPING = "weekend-damping"
  val WEEKEND_DAMPING_OFFSET = "weekend-damping-offset"
  val WEEKEND_DAMPING_SCALE = "weekend-damping-scale"

  val START_DATE = "start-date"
  val END_DATE = "end-date"
  val N_USERS = "n-users"
  val FIRST_USER_ID = "first-user-id"
  val GROWTH_RATE = "growth-rate"
  val TAG = "tag"

  def configFileLoader(fn: String) = {

    // simple JSON based state file format (to maximize readability)

    val s = Source.fromFile(fn)
    val rawContents = s.mkString
    val jsonContents = ujson.read(rawContents).obj
    s.close()

    jsonContents.get(ALPHA)
      .flatMap(v => v.numOpt)
      .foreach(alpha = _)

    jsonContents.get(BETA)
      .flatMap(v => v.numOpt)
      .foreach(beta = _)

    jsonContents.get(DAMPING)
      .flatMap(_.numOpt)
      .foreach(damping = _)

    jsonContents.get(WEEKEND_DAMPING)
      .flatMap(_.numOpt)
      .foreach(weekendDamping = _)

    jsonContents.get(WEEKEND_DAMPING_OFFSET)
      .flatMap(_.numOpt)
      .foreach(v => weekendDampingOffset = v.toInt) // in minutes

    jsonContents.get(WEEKEND_DAMPING_SCALE)
      .flatMap(_.numOpt)
      .foreach(v => weekendDampingScale = v.toInt) // in minutes

    jsonContents.get(SEED)
      .flatMap(_.numOpt)
      .foreach(v => seed = v.toLong)

    jsonContents.get(SESSION_GAP)
      .flatMap(_.numOpt)
      .foreach(v => sessionGap = v.toInt)

    jsonContents.get(NEW_USER_AUTH)
      .flatMap(_.strOpt)
      .foreach(newUserAuth = _)

    jsonContents.get(NEW_USER_LEVEL)
      .flatMap(_.strOpt)
      .foreach(newUserLevel = _)

    startDate = jsonContents.get(START_DATE)
      .flatMap(_.strOpt)

    endDate = jsonContents.get(END_DATE)
      .flatMap(_.strOpt)

    nUsers = jsonContents.get(N_USERS)
      .flatMap(_.numOpt)
      .map(_.toInt)

    firstUserId = jsonContents.get(FIRST_USER_ID)
      .flatMap(_.numOpt)
      .map(_.toInt)

    growthRate = jsonContents.get(GROWTH_RATE)
      .flatMap(_.numOpt)

    tag = jsonContents.get(TAG)
      .flatMap(_.strOpt)

    churnedState = jsonContents.get(CHURNED_STATE)
      .flatMap(_.strOpt)

    val states = new mutable.HashMap[(String,String,Int,String,String), State]

    val transitions: List[Value] = jsonContents.get(TRANSITIONS).flatMap(_.arrOpt).map(_.toList).getOrElse(List())

    for (t <- transitions) {
      val transition: LinkedHashMap[String, Value] = t.obj
      val source = readState(transition(SOURCE).obj)
      val dest = readState(transition(DEST).obj)
      val p      = transition(P).num

      if (!states.contains(source)) {
        states += (source ->
          new State(source))
      }
      if (!states.contains(dest)) {
        states += (dest ->
          new State(dest))
      }
      states(source)
        .addLateral(states(dest),p)
    }

    val initial: List[Value] = jsonContents.get(NEW_SESSION).flatMap(_.arrOpt).map(_.toList).getOrElse(List())
    for (i <- initial) {
      val item = i.obj
      val weight = item(WEIGHT).num.toInt
      val stateTuple = readState(item)
      val (_,auth,_,_,level) = stateTuple
      if (!initialStates.contains((auth,level)))
        initialStates.put((auth,level), new WeightedRandomThingGenerator[State]())
      if (!states.contains(stateTuple))
        throw new Exception("Unkown state found while processing initial states: " + stateTuple.toString())

      initialStates(auth,level).add(states(stateTuple), weight)
    }

    // TODO: put in check for initial state probabilities

    val showUserDetails: List[Value] = jsonContents.get(SHOW_USER_DETAILS).map(_.arr.toList).getOrElse(List())
    for (i <- showUserDetails) {
      val item = i.obj
      val auth = item(AUTH).str
      val show     = item(SHOW).bool
      showUserWithState += (auth -> show)
    }

    val levels = jsonContents.get(LEVELS).map(_.arr.toList).getOrElse(List())
    for (level <- levels) {
      val item = level.obj
      val levelName = item.get(LEVEL).flatMap(_.strOpt).getOrElse("")
      val levelWeight = item.get(WEIGHT).flatMap(_.numOpt).map(_.toInt).getOrElse(0)
      levelGenerator.add(levelName,levelWeight)
    }

    val auths = jsonContents.get(AUTHS).map(_.arr.toList).getOrElse(List())
    for (auth <- auths) {
      val item = auth.obj
      val levelName = item.get(AUTH).flatMap(_.strOpt).getOrElse("")
      val levelWeight = item.get(WEIGHT).flatMap(_.numOpt).map(_.toInt).getOrElse(0)
      authGenerator.add(levelName,levelWeight)
    }

  }

  private def readState(m: LinkedHashMap[String,Value]): (String, String, Int, String, String) =
    (m(PAGE).str,
     m(AUTH).str,
     m.get(STATUS).map(_.num.toInt).getOrElse(0),
     m.get(METHOD).map(_.str).getOrElse(""),
     m.get(LEVEL).map(_.str).getOrElse(""))

}
