``Human in the loop scenario
''starter command
python starter.py approval-test-01 --require-approval

``approval commands


python approve.py approval-test-01 network --approve --reason "network plan reviewed"

python approve.py approval-test-01 compute --approve --reason "compute plan reviewed"

python approve.py approval-test-01 app --approve --reason "app plan reviewed"


