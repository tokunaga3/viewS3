const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');

const s3 = new AWS.S3();
const BUCKET_NAME = process.env.BUCKET_NAME;
const OUTPUT_DIR = path.join(__dirname, 'output');

async function listFiles() {
    try {
        const data = await s3.listObjectsV2({ Bucket: BUCKET_NAME }).promise();
        const files = data.Contents.map(file => file.Key);
        console.log('Files in bucket:', files);

        for (const fileKey of files) {
            if (!fileKey.endsWith('.parquet') && !fileKey.endsWith('/')) {
                await downloadFile(fileKey);
            } else {
                console.log(`Skipping .parquet file or directory: ${fileKey}`);
            }
        }
    } catch (error) {
        console.error('Error fetching files:', error);
    }
}

async function downloadFile(fileKey) {
    try {
        const params = {
            Bucket: BUCKET_NAME,
            Key: fileKey
        };
        const data = await s3.getObject(params).promise();
        const filePath = path.join(OUTPUT_DIR, fileKey);

        // Ensure the output directory exists
        fs.mkdirSync(path.dirname(filePath), { recursive: true });

        // Check if the data is JSON
        let formattedData;
        try {
            formattedData = JSON.stringify(JSON.parse(data.Body.toString()), null, 2);
        } catch (e) {
            formattedData = data.Body.toString();
        }

        // Write the file to the output directory
        fs.writeFileSync(filePath, formattedData);
        console.log(`File downloaded: ${filePath}`);
    } catch (error) {
        console.error(`Error downloading file ${fileKey}:`, error);
    }
}

listFiles();