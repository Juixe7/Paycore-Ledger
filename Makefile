.PHONY: doctor up-lite up-full down seed demo chaos load verify evidence

ifeq ($(OS),Windows_NT)
  GRADLE := .\gradlew.bat
  DOCTOR_CMD := pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/doctor.ps1
else
  GRADLE := ./gradlew
  DOCTOR_CMD := bash scripts/doctor.sh
endif

doctor:
	$(DOCTOR_CMD)

up-lite:
	docker compose -f deploy/docker-compose.lite.yml up -d --build

up-full:
	pwsh -NoProfile -ExecutionPolicy Bypass -File deploy/k3d/up-full.ps1

down:
	docker compose -f deploy/docker-compose.lite.yml down -v --remove-orphans

seed:
	pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/seed.ps1

demo:
	pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/demo.ps1

chaos:
	$(GRADLE) :e2e-tests:test --tests "*Chaos*"

load:
	pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/load.ps1

verify:
	$(GRADLE) check

evidence:
	pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/evidence.ps1
