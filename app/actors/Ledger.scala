/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package actors

import java.util.UUID

import actors.accountant.{Poster, Accountant}
import akka.actor._
import akka.event.LoggingReceive
import com.github.nscala_time.time.Imports._
import play.api.libs.iteratee.Iteratee
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson._
import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import models._

class LedgerException(x: String) extends Exception(x)

object Ledger {
  case class NewPosting(count: Int, posting: Posting, uuid: UUID)

  case class GetPositions(account: Account)

  case class NewJournal(uuid: UUID, journal: Journal)

  case class JournalRecovery(journal: Journal)
  case object RecoveryDone

  def props: Props = Props(new Ledger)
}

object PostingGroup {
  case class AddPosting(count: Int, posting: Posting)
  case object JournalPersisted
  case object TimedOut

  def props(uuid: UUID, count: Int): Props = Props(new PostingGroup(uuid, count))

}

class PostingGroup(uuid: UUID, count: Int) extends Actor with ActorLogging with Stash {
  val timeoutCancellable = context.system.scheduler.scheduleOnce(Duration.create(5, TimeUnit.SECONDS), self, PostingGroup.TimedOut)
  val ledgerColl = MongoFactory.database[BSONCollection]("ledger")

  def waitForPersist(journal: Journal, postings: List[(Posting, ActorRef)]): Receive = LoggingReceive {
    case PostingGroup.JournalPersisted =>
      context.parent ! Ledger.NewJournal(uuid, journal)
      postings.foreach(_._2 ! Poster.PostingResult(uuid, result = true))
      journal.postings.foreach(SputnikEventBus.publish)
      context.stop(self)
  }

  def state(postings: List[(Posting, ActorRef)]): Receive = {
    def add(count: Int, posting: Posting): List[(Posting, ActorRef)] = {
      if (count != this.count)
        throw new LedgerException("count mismatch")
      else {
        (posting, sender()) :: postings
      }
    }

    def ready: Boolean = postings.size == count

    def toJournal(typ: String): Journal = {
      if (ready) {
        val journal = new Journal(typ, postings.map(_._1))
        if (!journal.audit)
          throw new LedgerException("Journal fails audit")
        journal
      }
      else
        throw new LedgerException("Postings not ready")
    }

    if (ready) {
      val journal = toJournal("")
      ledgerColl.insert(journal).map { lastError =>
        self ! PostingGroup.JournalPersisted
      }
      unstashAll()
      timeoutCancellable.cancel()
      waitForPersist(journal, postings)
    }
    else {
      LoggingReceive {
        case PostingGroup.AddPosting(c, p) =>
          context.become(state(add(c, p)))
        case PostingGroup.TimedOut =>
          postings.foreach(_._2 ! Poster.PostingResult(uuid, result = false))

        case msg =>
          log.debug(s"Stashing $msg")
          stash()
      }
    }


  }

  val receive = state(List())

}

object LedgerRecovery {
  def props: Props = Props(new LedgerRecovery)
}

class LedgerRecovery extends Actor with ActorLogging {
  val ledgerColl = MongoFactory.database[BSONCollection]("ledger")

  override def preStart() = {
    val enumerator = ledgerColl.find(BSONDocument()).cursor[Journal].enumerate()
    val processor: Iteratee[Journal, Unit] =
    {
      Iteratee.foreach { journal =>
        context.parent ! Ledger.JournalRecovery(journal)
      }
    }
    val f: Future[Unit] = enumerator |>>> processor
    f.onSuccess {
      case _ =>
        context.parent ! Ledger.RecoveryDone
        self ! Ledger.RecoveryDone
      }
  }

  val receive: Receive = {
    case Ledger.RecoveryDone =>
      context.stop(self)
  }
}


class Ledger extends Actor with ActorLogging with Stash {
  val receive = initializing(Queue())

  def initializing(ledger: Queue[Journal]): Receive = LoggingReceive {
    case Ledger.JournalRecovery(journal) =>
      context.become(initializing(ledger enqueue journal))
    case Ledger.RecoveryDone =>
      unstashAll()
      context.become(active(ledger, Map()))
    case msg =>
      log.debug(s"Stashing $msg")
      stash()
  }

  override def preStart() = {
    context.actorOf(LedgerRecovery.props)
  }

  def active(ledger: Queue[Journal], pending: Map[UUID, ActorRef]): Receive = {
    def getBalances(account: Account, timestamp: DateTime = DateTime.now): Positions = {
      val quantities = for {
        entry <- ledger
        posting@Posting(contract, a, _, _, _) <- entry.postings if account == a
        if entry.timestamp <= timestamp
      } yield (contract, posting.signedQuantity)
      quantities.groupBy[Contract](_._1).mapValues(_.map(_._2).sum)
    }

    LoggingReceive {
      case Ledger.NewPosting(count, posting, uuid) =>
        val newPending = if (!pending.contains(uuid)) pending + ((uuid, context.actorOf(PostingGroup.props(uuid, count), name = uuid.toString))) else pending
        newPending(uuid).tell(PostingGroup.AddPosting(count, posting), sender())
        context.become(active(ledger, newPending))

      case Ledger.GetPositions(user) =>
        sender ! getBalances(user)

      case Ledger.NewJournal(uuid, journal) =>
        context.become(active(ledger enqueue journal, pending - uuid))
    }
  }
}


