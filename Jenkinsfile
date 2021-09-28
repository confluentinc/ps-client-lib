properties([
        buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '30', numToKeepStr: '')),
        parameters([
            string(name: 'RELEASE_TAG', defaultValue: '')
        ])
])

node('docker-debian-jdk8') {
  stage('Source') {
    checkout([
        $class: 'GitSCM',
        branches: scm.branches,
        doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
        extensions: [[$class: 'CloneOption', noTags: false, shallow: false, depth: 0, reference: '']],
        userRemoteConfigs: scm.userRemoteConfigs,
    ])
  }
  stage('Build') {
      withMaven(
          globalMavenSettingsConfig: 'jenkins-maven-global-settings'
      ){
          withDockerServer([uri: dockerHost()]) {
             withEnv(['MAVEN_OPTS=-XX:MaxPermSize=128M']) {
                 sh "mvn --batch-mode -Pjenkins clean verify install dependency:analyze validate -U"
             }
          }
       }
  }
  def previous = currentBuild.previousBuild
  if ( currentBuild.currentResult == 'FAILURE' || ( previous != null && previous.currentResult == 'FAILURE' ) ) {
    slackNotify(
        status: currentBuild.currentResult,
        channel: '#cs-consulting-eng',
        always: false,
        dryrun: false,
    )
  }
}
