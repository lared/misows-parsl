# Configuration

1. Configure aws-cli (credentials are loaded from ~/.aws/credentials)
2. Generate an AMI relevant to the task at hand
3. Generate an ssh key
4. Fill out providerconfig.json

```
{
	"site": "EC2",
	"instancetype": "t2.nano",
	"nodeGranularity": 1,
	"maxNodes": 5,
	"AMIID": "ami-ae90b6cb",
	"AWSKeyName": "benglick-key"
}
```

5. Note that these images run on hardcoded regions (us-east-2)
6. Install boto3
7. Provide the path to the config file:

`EC2Provider('/path/to/config_file.json')`
