from sputnik.webserver.plugin import AuthenticationPlugin
from autobahn.wamp import types, util

class WAMPCRALogin(AuthenticationPlugin):
    def __init__(self):
        AuthenticationPlugin.__init__(self, u"cookie")
        self.cookies = {}

    @inlineCallbacks
    def onHello(self, router_session, realm, details):
        for authmethod in details.authmethods:
            if authmethod == u"wampcra":
                # Create and store a one time challenge.
                challenge = {"authid": details.authid,
                             "authrole": u"user",
                             "authmethod": u"wampcra",
                             "authprovider": u"database",
                             "session": details.pending_session,
                             "nonce": util.utcnow(),
                             "timestamp": util.newid()}

                router_session.challenge = challenge

                # We can accept unicode usernames, but convert them before
                # anything hits the database
                username = challenge["authid"].encode("utf8")

                # If the user does not exist, we should still return a
                #   consistent salt. This prevents the auth system from
                #   becoming a username oracle.
                noise = hashlib.md5("super secret" + username + "more secret")
                salt, secret = noise.hexdigest()[:8], "!"

                # The client expects a unicode challenge string.
                challenge = json.dumps(challenge, ensure_ascii = False)
                
                try:
                    router_session.exists = False

                    databases = self.manager.services["webserver.database"]
                    for db in database:
                        result = yield db.lookup(username)
                        if result:
                            break

                    if result:
                        salt, secret = result[0].split(":")
                        router_session.totp = result[1]
                        router_session.exists = True

                    # We compute the signature even if there is no such user to
                    #   prevent timing attacks.
                    router_session.signature = (yield threads.deferToThread( \
                            auth.compute_wcs, secret,
                            challenge.encode("utf8"))).decode("ascii")

                except Exception, e
                    error("Caught exception looking up user.")
                    error()

                # Client expects a unicode salt string.
                salt = salt.decode("ascii")
                extra = {u"challenge": challenge,
                         u"salt": salt,
                         u"iterations": 1000,
                         u"keylen": 32}

                returnValue(types.Challenge(u"wampcra", extra))

    def onAuthenticate(self, router_session, signature, extra):
        try:
            challenge = router_session.challenge
            if challenge == None:
                return
            if router_session.challenge.get("authmethod") != u"wampcra":
                return
            for field in ["authid", "authrole", "authmethod", "authprovider"]:
                if field not in challenge:
                    # Challenge not in expected format. It was probably
                    #   created by another plugin.
                    return

            if not router_session.challenge or not router_session.signature:
                return types.Deny(message=u"No pending authentication.")

            if len(signature) != len(router_session.signature):
                return types.Deny(message=u"Invalid signature.")

            success = True

            # Check each character to prevent HMAC timing attacks. This is
            #   really not an issue since each challenge gets a new nonce,
            #   but better safe than sorry.
            for i in range(len(router_session.signature)):
                if signature[i] != router_session.signature[i]:
                    success = False

            # Reject the user if we did not actually find them in the database.
            if not router_session.exist:
                success = False

            if success:
                return types.Accept(authid = challenge["authid"],
                        authrole = challenge["authrole"],
                        authmethod = challenge["authmethod"],
                        authprovider = challenge["authprovider"])

            return types.Deny(u"Invalid signature.")

        except:
            # let another plugin handle this
            return

