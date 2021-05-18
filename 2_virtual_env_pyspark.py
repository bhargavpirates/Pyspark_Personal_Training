1. Ceate virtual_env
	
	mkdir envs
	cd envs
	conda create --prefix /hc_dv/stage_work/<userid>/envs/<Name of the venv>  <specify python version>
	Ex: conda create --prefix /hc_dv/stage_work/ag25346/envs/venv-test-py-2.7 python=2.7

2. activate virtual_env
	
	source activate /hc_dv/stage_work/ag25346/envs/venv-test-py-2.7


3. Installing custom packages from git repo with setup file.
	
	conda config --set ssl_verify false
	pip --cert /opt/cloudera/security/CAChain.pem install pypandoc
	//Copy git clone http link of the feature branch and use it as below.
	pip --cert /opt/cloudera/security/CAChain.pem install git+<link>
	Ex: pip --cert /opt/cloudera/security/CAChain.pem install git+https://AG25346@bitbucket.anthem.com/scm/~af72092/whoop-etl.git

4. Ensure the required libraries are available in the venv created.
	
	Go through the below, in the repo.
	whoop-etl/requirements.txt
	whoop-etl/setup.py

	conda list


5. deactivate virtual_env
	
	conda deactivate

6. Create zip for the venv for distributing across the cluster while spark-submit

	cd /hc_dv/stage_work/ag25346/envs/venv-test-py-2.7
	zip -r /hc_dv/stage_work/ag25346/envs/venv-test-py-2.7.zip .



Git import the repository into venv or git pull after every code change committed in the repo
1.	While availing AWS S3 libraries, maintain python version as >=3.7 in the virtual env.
2.	Use VS Code as an IDE or any other, at your comfort. Develop, modify, commit and git push to the feature branch. For time being, in-order to perform testing rather directly connecting to S3 from VS Code, You can perform spark submit in the existing cluster.
3.	Please clone your repo to edge node, 
git pull every time after the code change,
Perform spark-submit with the venv deactivated.

In case of any change in the framework, You need to update the hcpipeline package and zip the venv again before performing the testing.
