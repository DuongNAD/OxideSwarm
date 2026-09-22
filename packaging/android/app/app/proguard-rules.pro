# ProGuard rules for OxideSwarm Worker App
-keepclasseswithmembers class * {
    native <methods>;
}

-keep class com.oxideswarm.worker.runner.OxideWorkerBridge { *; }
-keep class com.oxideswarm.worker.service.OxideWorkerService { *; }
