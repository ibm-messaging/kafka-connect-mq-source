from gitlint.rules import CommitRule, RuleViolation
import re


class SignedOffBy(CommitRule):
    id = "QP1"
    name = "body-requires-signed-off-by"

    sign_off_regex = re.compile("^(DCO 1.1 )?Signed-off-by: .* <[^@ ]+@[^@ ]+\.[^@ ]+>")

    def validate(self, commit):
        for line in commit.message.body:
            if self.sign_off_regex.match(line):
                return

        return [
            RuleViolation(
                self.id,
                "Body does not contain a valid 'Signed-off-by' line. See https://github.com/ibm-messaging/kafka-connect-mq-source/blob/master/CONTRIBUTING.md#legal",
                line_nr=1,
            )
        ]
