properties([
        buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '30', numToKeepStr: '')),
        parameters([
            string(name: 'RELEASE_TAG', defaultValue: '')
        ])
])

node('docker-oraclejdk8') {
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
      )
  }
}
