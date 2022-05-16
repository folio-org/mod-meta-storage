#!/bin/bash
cd ../server/target/
#jdeps fails because of a bug, see https://stackoverflow.com/questions/69943899/jdeps-cant-print-module-deps-due-to-a-multireleaseexception
#MODULES=$(jdeps -q --module-path compiler --multi-release 17 --print-module-deps  mod-meta-storage-server-fat.jar)
echo $MODULES
#we need additional modules for 'upgrade' modules, for some reason they are not reported by jdeps
#MODULES=$MODULES,jdk.internal.vm.ci,jdk.internal.vm.compiler,jdk.internal.vm.compiler.managemen
MODULES=java.base,java.sql,jdk.management,jdk.unsupported,org.graalvm.sdk,jdk.internal.vm.ci,jdk.internal.vm.compiler,jdk.internal.vm.compiler.management
echo $MODULES
rm -rf jre/
#generate JRE based on the module list
jlink --module-path compiler --verbose --compress 2 --strip-java-debug-attributes --no-header-files --no-man-pages --output jre --add-modules $MODULES
#launch the jar, we don't need to set modules paths anymore
jre/bin/java -XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI -jar mod-meta-storage-server-fat.jar
