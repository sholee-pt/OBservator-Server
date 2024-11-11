import boto3
import logging

# 로그 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 클라이언트 생성
s3 = boto3.client('s3')

# 버킷 이름 (사용자 버킷 이름으로 변경)
BUCKET_NAME = 'observator-bucket'

PASSWORD = ""  # 비밀번호 설정

def lambda_handler(event, context):
    logger.info("Lambda function has started.")

    # 'Records' 키가 event에 있는지 확인
    if 'Records' not in event:
        logger.warning("Event does not contain 'Records' key. Exiting function.")
        return {
            'statusCode': 400,
            'body': 'Invalid event format. No action taken.'
        }

    # 이벤트 객체에서 S3 버킷의 업로드된 파일 이름 확인
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        # index.html 파일이 업로드된 경우 함수 종료
        if object_key == 'index.html':
            logger.info("index.html detected. Exiting function to avoid recursion.")
            return {
                'statusCode': 200,
                'body': 'Recursion avoided. Lambda function exited successfully.'
            }

    try:
        # S3 버킷에서 모든 파일 목록 검색
        logger.info("Fetching file list from bucket...")
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME)
        
        # 파일 목록이 있는지 확인
        if 'Contents' in objects:
            files = [obj['Key'] for obj in objects['Contents'] if not obj['Key'].endswith('/') and obj['Key'] != 'index.html' and obj['Key'] != 'aws-programmatic-access-test-object']
            logger.info(f"File list: {files}")
        else:
            logger.info("No files found in the bucket.")
            files = []

        # HTML 구조 생성
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset=\"UTF-8\">
            <title>Protected Access</title>
            <link href=\"https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;700&display=swap\" rel=\"stylesheet\">
            <style>
                body {{
                    font-family: 'Roboto', sans-serif;
                    background-color: #f0f2f5;
                    color: #333;
                    line-height: 1.6;
                    padding: 20px;
                }}
                #protected-content {{
                    display: none;
                }}
                #password-input {{
                    font-size: 1rem;
                    padding: 10px;
                    margin-bottom: 10px;
                }}
                #submit-btn {{
                    padding: 10px 20px;
                    font-size: 1rem;
                    cursor: pointer;
                }}
                h2 {{
                    font-size: 2.5rem;
                    color: #0019f4;
                }}
                ul {{
                    list-style-type: none;
                    padding-left: 20px;
                }}
                li.folder-item {{
                    margin: 10px 0;
                }}
                li.file-item {{
                    display: inline;
                    margin-right: 15px;
                }}
                .folder {{
                    cursor: pointer;
                    font-weight: 700;
                    font-size: 1.2rem;
                    color: #2c3e50;
                    transition: background-color 0.3s, color 0.3s;
                    padding: 5px;
                    border-radius: 5px;
                }}
                .folder:hover {{
                    background-color: #e0e4e8;
                    color: #007bff;
                }}
                .folder:active {{
                    background-color: #cdd1d6;
                }}
                .file-link {{
                    font-size: 1rem;
                    color: #2980b9;
                    text-decoration: none;
                    margin: 5px 0;
                    display: inline-block;
                    transition: color 0.3s;
                }}
                .file-link:hover {{
                    text-decoration: underline;
                    color: #1c5d99;
                }}
                .nested {{
                    display: none;
                    margin-left: 20px;
                }}
                .active {{
                    display: block;
                }}
                .toggle-icon {{
                    margin-right: 8px;
                    font-size: 1.2rem;
                    transition: transform 0.3s;
                }}
                .toggle-icon.rotate {{
                    transform: rotate(90deg);
                }}
            </style>
            <script>
                function checkPassword() {{
                    var password = document.getElementById('password-input').value;
                    if (password === "{PASSWORD}") {{
                        document.getElementById('protected-content').style.display = 'block';
                        document.getElementById('password-section').style.display = 'none';
                    }} else {{
                        alert('Incorrect password.');
                    }}
                }}

                function toggleVisibility(id) {{
                    var element = document.getElementById(id);
                    var icon = document.querySelector('[data-target="' + id + '"]');
                    if (element.classList.contains("active")) {{
                        element.classList.remove("active");
                        icon.classList.remove("rotate");
                    }} else {{
                        element.classList.add("active");
                        icon.classList.add("rotate");
                    }}
                }}
            </script>
        </head>
        <body>
            <div id="password-section">
                <h2>Enter Password to Access</h2>
                <input type="password" id="password-input" placeholder="Enter password">
                <button id="submit-btn" onclick="checkPassword()">Submit</button>
            </div>
            <div id="protected-content">
                <h2>OBservator Bucket</h2>
                <ul>
        """

        # 트리 구조를 담을 딕셔너리
        file_tree = {}

        # 파일 경로를 트리 구조로 정리
        for file in files:
            parts = file.split('/')
            current = file_tree

            for part in parts:
                if part not in current:
                    current[part] = {}
                current = current[part]

        # 트리 구조를 HTML로 변환하는 함수
        def generate_html(tree, path=""):
            html = "<ul>"
            for key, value in tree.items():
                current_path = f"{path}/{key}".strip("/")
                if isinstance(value, dict) and value:  # 폴더
                    html += f'<li class="folder-item"><span class="toggle-icon" data-target="{current_path}" onclick="toggleVisibility(\'{current_path}\')">&#9654;</span><span class="folder" onclick="toggleVisibility(\'{current_path}\')">{key}</span>'
                    html += f'<ul id="{current_path}" class="nested">'
                    html += generate_html(value, current_path)
                    html += "</ul></li>"
                else:  # 파일
                    html += f'<li class="file-item"><a class="file-link" href="https://{BUCKET_NAME}.s3.amazonaws.com/{current_path}">{key}</a></li>'
            html += "</ul>"
            return html

        # 생성한 트리 구조를 HTML 콘텐츠에 추가
        html_content += generate_html(file_tree)
        html_content += """
                </ul>
            </div>
        </body>
        </html>
        """

        # index.html 파일을 S3 버킷에 업로드
        logger.info("Uploading index.html file...")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key='index.html',
            Body=html_content,
            ContentType='text/html',
            ACL='public-read'  # 퍼블릭 읽기 권한 추가
        )
        logger.info("index.html file successfully uploaded.")
    
    except Exception as e:
        logger.error(f"Error occurred: {e}")

    return {
        'statusCode': 200,
        'body': 'index.html updated successfully!'
    }
