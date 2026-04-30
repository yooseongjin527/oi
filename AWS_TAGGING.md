# OI 프로젝트 — AWS 리소스 태그 표준

공유 AWS 계정 환경에서 본인 리소스 식별 + Cost Explorer 필터링용.

## 필수 태그

| Key | Value | 비고 |
|---|---|---|
| `Project` | `oi` | 고정 |
| `Owner` | `de-ai-17` | 본인 IAM 사용자명 |
| `Environment` | `dev` / `prod` | dev=로컬, prod=EC2 |
| `ManagedBy` | `manual` / `terraform` | 향후 IaC 전환 시 변경 |

## 적용 대상

- S3 버킷 (`oi-data-lake-*`)
- 향후 EC2 인스턴스 (Day 4 또는 Day 9)
- VPC, Subnet, IGW, Security Group (Day 4)
- IAM Role (`oi-ec2-role`, Day 4)

## Cost Explorer 필터 (수동 체크 루틴)

1. AWS Console → Billing → Cost Explorer
2. Filters → Tag → `Project: oi`
3. 일별 그래프로 본인 프로젝트 비용만 조회

## 일일 체크 루틴 (작업 시작 시 1분)

```bash
# 현재 활성 리소스 확인 (ap-northeast-2)
aws s3 ls | grep oi-
aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=oi" "Name=instance-state-name,Values=running,pending" \
  --query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name]' \
  --output table
```

## 작업 종료 루틴 (작업 끝낼 때)

- EC2: stop (terminate 아님 — 다음 날 재사용)
- 일시 리소스: 즉시 삭제
- S3: 그대로 둠 (라이프사이클이 알아서 정리)
