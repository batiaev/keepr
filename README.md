[![Build](https://github.com/batiaev/keepr/actions/workflows/gradle.yml/badge.svg?branch=main)](https://github.com/batiaev/keepr/actions/workflows/gradle.yml)

# Keepr

Tool for FX rates fetching from multiple providers

## Build
```./gradlew clean build```

## Run
- Fetch from Central Bank of Russia
```java -jar ./build/libs/keepr-1.0-SNAPSHOT.jar CBR ./fx.sqlite```
- Fetch from European Central Bank
  ```java -jar ./build/libs/keepr-1.0-SNAPSHOT.jar ECB ./fx.sqlite```
- Fetch from Open Exchange Rate
  ```java -jar ./build/libs/keepr-1.0-SNAPSHOT.jar OXR ./fx.sqlite OXR_TOKEN```

## Dependencies
- OXR client
- CBR client
- ECB client
- Revolut client
- SQLite

## Author
Anton Batiaev <anton@batiaev.com>
