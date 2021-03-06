/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package actors

import akka.actor.{Props, ActorRef, Actor}
import models._

trait GetOrCreateChild extends Actor {
  def getOrCreateChild[KeyType <: Nameable](key: KeyType, onCreate: ActorRef => Unit = actor => ())(implicit factory: (KeyType) => Props) = {
    context.child(key.name) match {
      case Some(actor: ActorRef) => actor
      case None =>
        val actor = context.actorOf(factory(key), name = key.name)
        onCreate(actor)
        actor
    }
  }
}