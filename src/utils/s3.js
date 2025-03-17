const AWS = require('aws-sdk');

const s3 = new AWS.S3();

const BUCKET_NAME = process.env.BUCKET_NAME;;

const getFilesFromBucket = async () => {
    const params = {
        Bucket: BUCKET_NAME,
    };

    try {
        const data = await s3.listObjectsV2(params).promise();
        return data.Contents;
    } catch (error) {
        console.error('Error fetching files from S3:', error);
        throw error;
    }
};

module.exports = {
    getFilesFromBucket,
};