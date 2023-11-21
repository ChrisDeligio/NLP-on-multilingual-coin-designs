pipeline {
  agent any
  stages {
    stage('Prepare data') {
      parallel {
        stage('Prepare data') {
          steps {
            sh '''mkdir -p /Users/chris/Dev/Docker/jenkins/Workspace/Code/
cp -r * /Users/chris/Dev/Docker/jenkins/Workspace/Code'''
          }
        }

        stage('Unit Test Functions') {
          steps {
            sh '''if docker image inspect ner_model:latest --format="ignore me"
then 
	echo "using latest image"
else
	docker build -t ner_model -f /Users/chris/Dev/Docker/jenkins/Workspace/Code/Docker/Dockerfile .
fi
if docker ps -l | grep ner_model
then
	echo "... ner_model running ..."
else
	echo "... ner_model not running ..."
    echo "building ..."
    docker run --name ner_model_train -p 5001:5001 -v /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/:/code/host  ner_model python host/Docker/unit_test.py
    docker rm ner_model_train
fi
'''
          }
        }

      }
    }

    stage('Train NER') {
      steps {
        sh '''if docker image inspect ner_model:latest --format="ignore me"
then 
	echo "using latest image"
else
	docker build -t ner_model -f /Users/chris/Dev/Docker/jenkins/Workspace/Code/Docker/Dockerfile .
fi
if docker ps -l | grep ner_model
then
	echo "... ner_model running ..."
else
	echo "... ner_model not running ..."
    echo "building ..."
    docker run --name ner_model_train -p 5001:5001 -v /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/:/code/host  ner_model python host/Docker/train_ner.py
    docker rm ner_model_train
fi

result=$(cat /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/log/f1_score.txt)
test=98
if (($result>$test))
then	
	echo "Passed"
else
	echo "Failed"
fi'''
      }
    }

    stage('Evaluation Report Step 1') {
      steps {
        sh 'cp /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/log/logger.html /Users/chris/Sites/ML'
      }
    }

    stage('Test NER') {
      steps {
        sh '''if docker image inspect ner_model:latest --format="ignore me"
then 
	echo "using latest image"
else
	docker build -t ner_model -f /Users/chris/Dev/Docker/jenkins/Workspace/Code/Docker/Dockerfile .
fi
if docker ps -l | grep ner_model
then
	echo "... ner_model running ..."
else
	echo "... ner_model not running ..."
    echo "building ..."
    docker run --name ner_model_test -p 5001:5001 -v /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/:/code/host  ner_model python host/Docker/test_ner.py
    docker rm ner_model_test
fi

result=$(cat /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/log/test_f1_score.txt)
test=98
if (($result>$test))
then	
	echo "Passed"
else
	echo "Failed"
fi'''
      }
    }

    stage('Evaluation Report Step 2') {
      steps {
        sh 'cp /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/log/test_log.html /Users/chris/Sites/ML'
      }
    }

    stage('Train RE') {
      steps {
        sh '''if docker image inspect ner_model:latest --format="ignore me"
then 
	echo "using latest image"
else
	docker build -t ner_model -f /Users/chris/Dev/Docker/jenkins/Workspace/Code/Docker/Dockerfile .
fi
if docker ps -l | grep ner_model
then
	echo "... ner_model running ..."
else
	echo "... ner_model not running ..."
    echo "building ..."
    docker run --name re_model_train -p 5001:5001 -v /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/:/code/host  ner_model python host/Docker/train_re.py
    docker rm re_model_train
fi

result=$(cat /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/log/re_f1_score.txt)
test=80
if (($result>$test))
then	
	echo "Passed"
else
	echo "Failed"
fi'''
      }
    }

    stage('Evaluation Report RE') {
      steps {
        sh 'cp /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/log/re_logger.html /Users/chris/Sites/ML'
      }
    }

    stage('Publish') {
      steps {
        sh '''result=$(cat /Users/chris/Dev/Repository/NLP-on-multilingual-coin-descriptions/log/test_f1_score.txt)
test=98
if (($result>$test))
then	
	echo "Published"
else
    echo "Not Published"
fi'''
      }
    }

  }
}