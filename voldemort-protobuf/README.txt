This is a place holder directory for injecting custom gradle behavior.

This directory is not directly used in the open source gradle builds.
The open source project produces multiple artifacts voldemort.jar and 
voldemort-contrib.jar and few others. 

This is not gradle friendly as in gradle if
you are producing more than one jar from the same module exporting
ivy dependencies and other becomes very trickier. 

This empty directory allows an upstream wrapper to inject custom behavior. 
For example upstream wrapper can define a project(':voldemort:voldemort-protobuf')
and define all the gradle behavior dependencies, black box build . 