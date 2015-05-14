This is a place holder directory for injecting custom gradle behavior.

This directory is not directly used in the open source gradle builds.
The open source project produces 2 artifacts voldemort.jar and 
voldemort-contrib.jar. This is not gradle friendly as in gradle if
you are producing more than one jar from the same module exporting
ivy dependencies and other becomes very trickier. 

This allows an upstream wrapper to inject custom behavior. For example
upstream wrapper can define a project(':voldemort:voldemort-contrib')
and define all the gradle behavior dependencies, black box build . 

Theoretically we could have used the contrib directory but that will
result in lot more confusion as that directory produces some artifacts
and upstream could want a different behavior.