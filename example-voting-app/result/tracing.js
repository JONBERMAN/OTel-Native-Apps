const { NodeSDK } = require('@opentelemetry/sdk-node');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');
const { Resource } = require('@opentelemetry/resources');
const { SemanticResourceAttributes } = require('@opentelemetry/semantic-conventions');

const sdk = new NodeSDK({
  resource: new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: 'result-app',
  }),
  // 실제 OTel Collector 주소로 변경해주세요
  traceExporter: new OTLPTraceExporter({
    url: 'http://otlp-exporter.monitoring.svc.cluster.local:4318/v1/traces'
  }),
  // express, http, pg 등 많이 쓰이는 라이브러리를 자동 계측합니다
  instrumentations: [getNodeAutoInstrumentations()]
});

sdk.start();
console.log('OpenTelemetry initialized for Result App');