# Contributing to MicroRaft

By contributing code to the MicroRaft project in any form, including sending 
a pull request via Github, a code fragment or patch by any public means, you
agree to release your code under the terms of the Apache 2.0 license that you
can find in the MicroRaft repository. 


## How to Report an Issue

If you want to report an issue or a bug, please provide details, such as Java
version, JVM parameters, logs or stack traces, operation system, and steps to 
reproduce your issue. I would be grateful If you could include a unit or an 
integration test as a reproducer.


## How to Discuss a Feature Request

You can join our [Slack group](https://join.slack.com/t/microraft/shared_invite/zt-dc6utpfk-84P0VbK7EcrD3lIme2IaaQ) 
to discuss your ideas or feature requests. You can also create a Github issue 
to formalize your feature request.  


## How to Ask a Question

Similarly, you can join our [Slack group](https://join.slack.com/t/microraft/shared_invite/zt-dc6utpfk-84P0VbK7EcrD3lIme2IaaQ)
to ask your questions.


## How to Provide a Code Change

No direct commits are allowed to the MicroRaft repository. If you want to 
provide a code change:

1. Fork MicroRaft on Github,
2. Create a branch for your code change, 
3. Push to your branch on your fork,
4. Create a pull request to the MicroRaft repository.

MicroRaft contains `checkstyle` and `spotbugs` tools for static code analysis.
Please run the following commands locally before issuing your pull request.

`mvn clean validate -P checkstyle`

`mvn clean compile -P spotbugs`

Thanks for your help and effort!
