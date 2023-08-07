#!/bin/bash
export AWS_PACKAGES=(
  "bundle"
  "url-connection-client"
)

for pkg in "${AWS_PACKAGES[@]}"; do
    export DEPENDENCIES+=",$AWS_MAVEN_GROUP:$pkg:$AWS_SDK_VERSION"
done

# Set space as the delimiter
IFS=','

#Read the split words into an array based on space delimiter
read -a deparr <<< "$DEPENDENCIES"


for dep in "${deparr[@]}"; do
    mvn dependency:get -Dartifact=$dep
done

find /root/.m2/. -iname *.jar -exec cp -r '{}' /opt/spark/jars \;
