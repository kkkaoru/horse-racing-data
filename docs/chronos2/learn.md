# Chronos2 MLX — M5 Pro Max学習・推論高速化 / Production Export 実装指示書

対象リポジトリ:

```text
https://github.com/tsfm-ai/chronos2-mlx
```

# 0. 最終目標

この実装では、単純なMLX推論高速化だけではなく、以下の一連のワークフローを完成させる。

```text
Local Mac
M5 Pro Max
    │
    ├─ MLXで高速fine-tuning
    │   ├─ LoRA
    │   ├─ QLoRA
    │   ├─ head fine-tuning
    │   └─ 必要ならfull fine-tuning
    │
    ├─ M5 Pro Max向けtraining最適化
    │
    ▼
学習済みモデル
    │
    ├─ LoRA fuse
    ├─ MLX固有形式を除去
    ├─ Chronos-2互換weightへ変換
    └─ standard safetensorsとしてexport
    │
    ▼
Production
    │
    ├─ Linux CPU
    ├─ PyTorch
    ├─ ONNX Runtime
    ├─ OpenVINO等
    └─ 将来の他ランタイム
```

**MLXは学習・Mac上の検証を高速化するために使用する。**

本番モデル自体をMLX依存にはしない。

---

# 1. 設計原則

以下を分離すること。

## Training optimization

```text
M5 Pro Max
+
MLX
```

に特化してよい。

## Model artifact

MLXに依存してはいけない。

## Production optimization

本番環境に応じて別途最適化する。

つまり、

```text
Training runtime
≠
Model format
≠
Production runtime
```

とする。

---

# 2. 現在のtraining実装の問題点

対象:

```text
src/chronos2_mlx/train.py
```

現状は概ね、

```python
loss_and_grad = nn.value_and_grad(model, loss_fn)

for context, future_target in train_loader:
    loss, grads = loss_and_grad(
        model,
        context,
        future_target,
    )

    grads, _ = optim.clip_grad_norm(
        grads,
        config.grad_clip,
    )

    optimizer.update(model, grads)

    mx.eval(
        model.parameters(),
        optimizer.state,
    )
```

となっている。

以下が未最適化。

```text
training step全体のmx.compile
bf16 mixed precision policy
gradient accumulation
optimizer memory削減
QLoRA専用training path
static batch shape
DataLoader CPU overhead
NumPy → MLX変換
prefetch
training benchmark
portable model export
```

また、

```text
warmup_steps
```

がTrainConfigに存在するが、現在のtraining loopでは実際のlearning-rate warmupとして利用されていない。

これも修正対象とする。

---

# 3. 最優先: training step全体をmx.compile

現在、

```python
loss_and_grad = nn.value_and_grad(
    model,
    loss_fn,
)
```

を直接呼んでいる。

これを、

```text
forward
+
loss
+
backward
+
可能ならoptimizer update
```

まで含めてcompile可能か検証する。

---

## 推奨構造

まず、

```python
loss_and_grad = nn.value_and_grad(
    model,
    loss_fn,
)
```

を作る。

次に、

```python
def train_step(
    model,
    optimizer,
    context,
    target,
):
    loss, grads = loss_and_grad(
        model,
        context,
        target,
    )

    if grad_clip > 0:
        grads, _ = optim.clip_grad_norm(
            grads,
            grad_clip,
        )

    optimizer.update(
        model,
        grads,
    )

    return loss
```

相当の処理をcompileする。

最新MLX APIでoptimizer stateを含むcompileが安全に行えるか確認すること。

不可能または不安定なら、

```text
forward + backward
```

のみをcompileし、

```text
optimizer.update
```

はcompile外でもよい。

---

# 4. compile対象を段階的にbenchmarkする

以下3種類を比較する。

```text
A
compileなし

B
loss_and_gradのみcompile

C
full train step compile
```

必ず、

```text
samples/sec
steps/sec
step latency
peak memory
compile latency
```

を比較する。

速度が上がらない方法は採用しない。

---

# 5. static shape化

`mx.compile` の効果を最大化するため、training input shapeを固定する。

基本training shape:

```text
[batch_size, context_length]
[batch_size, prediction_length]
```

を固定する。

---

## 最終batch問題

現在DataLoaderは最後のbatchが小さくなる可能性がある。

これによってshapeが変化し、

```text
recompile
```

が発生する可能性がある。

training時には、

```text
drop_last=True
```

相当を導入する。

設定例:

```python
TrainConfig(
    static_batch=True,
    drop_last=True,
)
```

validationではdrop_lastしなくてもよい。

---

# 6. context lengthもstaticにする

training時は、

```text
context_length = 512
```

などに固定する。

現状 `_rolling_windows()` が既に、

```text
左NaN padding
```

によって固定context lengthを生成しているため、この性質を維持する。

これはcompileには有利。

---

# 7. bf16 mixed precision training

M5 Pro Maxでは、

```text
bf16
```

をtrainingの第一候補とする。

目標:

```text
weights:
bf16

activations:
bf16

matrix multiplication:
bf16

sensitive reductions:
fp32

loss:
fp32

normalization statistics:
fp32
```

---

# 8. FP32を維持する処理

少なくとも以下はfp32維持を検証する。

```text
InstanceNorm mean
InstanceNorm variance
RMSNorm variance
pinball loss accumulation
gradient norm
optimizer moments
```

単純にモデル全体をbf16へcastして終わりにしない。

---

# 9. training用dtype設定

TrainConfigへ追加。

```python
@dataclass
class TrainConfig:
    ...

    dtype: str = "bfloat16"

    mixed_precision: bool = True
```

可能なら、

```text
float32
bfloat16
```

を選択可能にする。

fp16は実験対象にはしてよいが、

```text
M5 Pro Max recommended
```

にはしない。

---

# 10. master weightsについて検証

full fine-tuningの場合、

```text
bf16 parameter
+
bf16 update
```

だけでは更新精度が不足する可能性がある。

以下を比較する。

```text
A
bf16 parameter直接更新

B
fp32 master weights
+
bf16 forward
```

ただしBはmemoryが増える。

LoRAではtrainable parameter量が少ないので、

```text
LoRA parameters = fp32
base model = bf16
activations = bf16
```

も有力。

実測・精度比較して決定する。

---

# 11. Gradient Accumulation

TrainConfigへ追加。

```python
gradient_accumulation_steps: int = 1
```

例えば、

```text
micro batch = 16

gradient accumulation = 4

effective batch = 64
```

とする。

---

# 12. accumulation実装

概念的には、

```python
accum_grads = zero_grad_tree()

for micro_step in range(
    gradient_accumulation_steps
):
    loss, grads = loss_and_grad(...)

    accum_grads += grads

accum_grads /= gradient_accumulation_steps

optimizer.update(
    model,
    accum_grads,
)
```

とする。

gradient clippingは原則、

```text
accumulation完了後
```

に行う。

つまり、

```text
micro batch
↓
gradient

micro batch
↓
gradient

micro batch
↓
gradient

sum / average
↓
clip
↓
optimizer update
```

とする。

---

# 13. Gradient Accumulationの目的

これは単なるメモリ節約ではない。

M5 Pro Maxで、

```text
physical batch size
```

をmemory上安全なサイズに抑えつつ、

```text
effective batch size
```

を大きくする。

例:

```text
batch_size=16
accumulation=8

effective batch=128
```

---

# 14. accumulationとcompile

以下を比較する。

```text
compileされたmicro-step
+
Python accumulation loop
```

vs

```text
accumulation loop自体もcompile
```

MLXで後者が安定しない場合は前者を採用する。

過度な複雑化はしない。

---

# 15. Optimizer state memory削減

現在:

```python
optim.AdamW(...)
```

を使用している。

Adam系optimizerは基本的に、

```text
parameter
+
first moment
+
second moment
```

を保持するためfull fine-tuningではmemory使用量が大きい。

---

# 16. LoRA/QLoRAではtrainable paramsだけstateを持つ

最重要。

optimizerには、

```text
model.trainable_parameters()
```

のみを対象とする。

base modelについてoptimizer stateを絶対に生成しない。

以下をテストする。

```text
LoRA trainable params
vs
optimizer state parameter count
```

が一致すること。

---

# 17. optimizer候補比較

full fine-tuningの場合のみ、

```text
AdamW
Adafactor等の省メモリoptimizer
```

を現在のMLXが提供している範囲で比較する。

比較項目:

```text
peak memory
step/sec
loss convergence
validation loss
```

AdamWを無条件に捨てない。

---

# 18. optimizer dtype

AdamW stateがfp32である場合、

精度面では望ましい一方、

```text
memory
```

を消費する。

MLXが安全に低精度optimizer stateを扱えるAPIを提供している場合のみ、

```text
bf16 optimizer state
```

を実験する。

独自のunsafeな低精度Adamを実装しない。

---

# 19. QLoRA専用path

現在のgeneric LoRA trainingとは別に、

```text
QLoRA mode
```

を明示的に扱う。

TrainConfig:

```python
finetune_mode:
    "lora"
    "qlora"
    "head"
    "full"
```

を検討する。

---

# 20. QLoRAのbase model

QLoRAでは、

```text
base weights
=
int4 または int8
```

とする。

その上に、

```text
LoRA A
LoRA B
```

のみtrainable parameterとして追加する。

絶対条件:

```text
quantized base weights:
frozen

quantization scales:
frozen

quantization biases:
frozen

LoRA:
trainable
```

---

# 21. QLoRA base固定テスト

テスト前後で、

```text
quantized base weight
scale
bias
```

のhashまたはtensor値を比較する。

学習前後で完全一致すること。

変更されてよいのは、

```text
LoRA A
LoRA B
```

のみ。

---

# 22. QLoRA training dtype

候補:

```text
base weights:
int4

LoRA:
bf16 または fp32

activations:
bf16

loss:
fp32

optimizer states:
LoRA paramsのみ
```

この構成を最優先benchmark対象にする。

---

# 23. LoRA fuse

local Macで学習終了後、

```text
base
+
LoRA
```

をfuse可能にする。

既存の、

```python
fuse_lora(model)
```

を利用・改善する。

fuse前後でprediction parityを検証する。

目標:

```text
max absolute difference
```

をdtypeに応じた許容範囲内にする。

---

# 24. QLoRA exportの重要事項

QLoRAでint4 baseへLoRAを適用したモデルを、

そのMLX量子化形式のままproduction artifactとしない。

Production export時には原則、

```text
dequantize base
↓
LoRA deltaをfuse
↓
標準precision weight作成
↓
safetensors export
```

というpathを用意する。

必要ならその後、

```text
production runtime側
```

で再量子化する。

---

# 25. なぜproduction側で再量子化するのか

MLX int4形式は、

```text
MLX実行向け
```

であって、

```text
ONNX Runtime
OpenVINO
PyTorch CPU
```

などの最適量子化形式とは異なる。

したがって、

```text
MLX quantization
```

をproduction artifact contractにしない。

---

# 26. Portable model export

新規module候補:

```text
src/chronos2_mlx/export.py
```

以下を提供する。

```python
export_pretrained(
    model,
    output_dir,
    ...
)
```

---

# 27. export成果物

最低限、

```text
output/
├── config.json
├── model.safetensors
├── generation/config metadata if required
└── export_metadata.json
```

とする。

Chronos-2標準実装から読み込めることを目標とする。

---

# 28. export metadata

`export_metadata.json` に、

```json
{
  "source": "chronos2-mlx",
  "base_model": "amazon/chronos-2",
  "training_mode": "qlora",
  "training_dtype": "bfloat16",
  "lora_fused": true,
  "mlx_quantization_removed": true
}
```

相当を記録する。

---

# 29. Production parity test

最重要テスト。

同じ入力について、

```text
MLX local model
```

と、

```text
export済み standard Chronos-2 model
```

で推論する。

予測結果を比較する。

---

# 30. artifact contract

学習artifactの正本は、

```text
standard Chronos-2 compatible safetensors
```

とする。

以下を正本にしない。

```text
MLX QuantizedLinear内部形式
MLX Python object
pickle
Apple Silicon依存binary
```

---

# 31. checkpoint設計

training途中ではMLX最適化形式を利用してよい。

例えば:

```text
checkpoints/
step-100/
step-200/
```

ではMLX向けstateを利用可能。

しかしfinal exportは必ずportable形式へ変換する。

---

# 32. Resume training

checkpointには、

```text
model state
LoRA state
optimizer state
current step
learning rate scheduler state
random seed
```

を保存できるようにする。

M5 Pro Maxで学習を中断しても再開可能にする。

---

# 33. learning rate warmup修正

現在TrainConfigには、

```python
warmup_steps: int = 100
```

が存在する。

しかし実際のoptimizer learning rateには反映されていない。

必ず実装する。

---

# 34. 推奨scheduler

最低限、

```text
linear warmup
↓
constant
```

または、

```text
linear warmup
↓
cosine decay
```

を選択可能にする。

例:

```python
lr_scheduler: str = "cosine"
warmup_steps: int = 100
```

---

# 35. DataLoader改善

現在、

```text
all rolling windows
↓
NumPy arrays
↓
各batchでadvanced indexing
↓
mx.array()
```

となっている。

各iterationで、

```python
mx.array(
    self.contexts[idx]
)
```

を作るためconversion overheadが発生する。

---

# 36. DataLoader optimization Phase A

まずNumPy allocationを減らす。

現在:

```python
windows = []

for ...:
    windows.append(
        (ctx, target)
    )

self.contexts = np.stack(...)
self.targets = np.stack(...)
```

大量datasetではメモリを二重に使う可能性がある。

直接preallocateできるなら変更する。

---

# 37. DataLoader optimization Phase B

datasetがmemoryへ収まる場合、

初期化時に一度、

```text
NumPy
↓
MLX array
```

へ変換する方式をbenchmarkする。

例えば:

```python
self.contexts_mx = mx.array(
    self.contexts
)

self.targets_mx = mx.array(
    self.targets
)
```

その後、

```text
MLX上でindex selection
```

する。

---

# 38. Unified Memoryの活用

Apple SiliconはCPU/GPU unified memoryである。

そのため、

```text
NumPy → MLX
```

のコストはDiscrete GPU環境とは異なるが、

Python allocationおよびarray construction overheadは残る。

必ず実測する。

---

# 39. 巨大datasetの場合

全datasetをMLX array化してmemory pressureが大きい場合は、

```text
streaming dataset
```

を用意する。

2モードを検討。

```text
memory dataset
streaming dataset
```

TrainConfig例:

```python
dataset_mode: str = "auto"
```

---

# 40. Prefetch

CPU preprocessingとGPU executionを可能な範囲でoverlapする。

MLX/Python環境で安全に実現できる場合、

```text
current batch
→ GPU

next batch
→ CPU preprocessing
```

を行う。

ただしcomplexityの割に効果が小さければ採用しない。

---

# 41. rolling window生成最適化

Python generatorで1windowずつ、

```python
np.full(...)
copy
yield
```

する現在の構造もbenchmark対象。

可能ならNumPy stride/view等を利用して、

```text
rolling window extraction
```

をvectorizeする。

ただし、

```text
NaN padding
```

およびデータ意味論を変えない。

---

# 42. training用fused kernels

推論と同様、trainingでも以下を利用する。

優先順位:

```text
P0
mx.fast.scaled_dot_product_attention

P0
mx.fast.rms_norm

P1
mx.fast.rope
```

ただしbackward対応を必ず確認する。

---

# 43. fused Attention

現在manual:

```text
QK^T
softmax
V
```

を、

```python
mx.fast.scaled_dot_product_attention(
    q,
    k,
    v,
    scale=1.0,
    mask=mask,
)
```

へ置換できるか検証する。

Chronos-2固有の、

```text
scale=1.0
```

を維持する。

---

# 44. backward parity

推論結果だけでなく、

```text
gradient parity
```

を確認する。

manual Attentionとfused Attentionで、

同一input・同一parameterについて、

```text
loss
gradient
```

を比較する。

---

# 45. activation memory

trainingでは推論以上に、

```text
activation memory
```

が問題になる。

以下を計測する。

```text
batch
context length
number of layers
attention sequence
```

ごとのpeak memory。

---

# 46. Gradient checkpointing

full fine-tuning時は、

```text
gradient checkpointing
```

またはMLXで相当するcheckpoint/recompute機構が利用可能か確認する。

利用可能なら、

```text
encoder block単位
```

でcheckpointすることを検討。

目的:

```text
activation memory ↓
compute ↑
```

M5 Pro Maxの大きな演算性能を使い、

memoryとcomputeを交換する。

---

# 47. LoRAではcheckpointingを必須にしない

LoRA/QLoRAはfull fine-tuningよりmemory消費が小さいため、

checkpointingによるrecompute overheadの方が大きい可能性がある。

実測で決める。

---

# 48. M5 Pro Max向けAuto Tuning

ハードコードされたbatch sizeを推奨しない。

training開始前にオプションで、

```text
batch size auto benchmark
```

を実行できるようにする。

候補:

```text
8
16
32
64
128
```

---

# 49. Auto batch探索

以下の条件を満たす最大または最速batchを探索する。

```text
memory安全域を維持
+
最高samples/sec
```

単純に最大batchを選ばない。

---

# 50. memory safety margin

48GB unified memory全部を学習へ使わない。

OSや他アプリを考慮する。

例えば、

```text
利用可能memoryの80〜85%
```

程度を上限候補とする。

ただし固定値ではなくconfig可能にする。

---

# 51. 推奨training preset

最終benchmark結果からpresetを定義する。

## LoRA Fast

```text
base:
bf16

LoRA:
bf16 or fp32

compile:
on

static shape:
on

gradient accumulation:
auto

fused kernels:
on
```

## QLoRA Memory Efficient

```text
base:
int4 frozen

LoRA:
bf16

activations:
bf16

loss:
fp32

compile:
on

gradient accumulation:
auto
```

## Full Fine-tune Quality

```text
base:
bf16

optimizer:
AdamW or benchmark winner

gradient checkpoint:
optional

compile:
on

static shape:
on
```

---

# 52. Training benchmark matrix

最低限、

```text
mode:
LoRA
QLoRA
full

dtype:
fp32
bf16

compile:
off
on

batch:
8
16
32
64

context:
128
512
2048
```

を比較する。

full fine-tuningについて非現実的な組合せはskip可能。

---

# 53. Training benchmark metrics

必須:

```text
step time
samples/sec
windows/sec
peak unified memory
initial compile time
warm step latency
CPU utilization
GPU utilization
final train loss
validation loss
```

---

# 54. 最重要benchmark

以下を必ず比較する。

```text
Baseline
現在のtrain.py
```

vs

```text
Optimized
bf16
+
fused attention
+
mx.compile
+
static shape
```

vs

```text
Optimized QLoRA
int4 base
+
bf16 LoRA
+
mx.compile
```

---

# 55. Training精度を犠牲にしない

高速化前後で、

```text
同一seed
同一dataset
同一training steps
```

を使用する。

比較:

```text
training loss
validation loss
forecast metrics
```

速度だけで採用しない。

---

# 56. inference optimizationも維持

学習高速化と並行して、production候補モデルのMac上検証用に以下も実装する。

優先順位:

```text
P0
mx.fast.scaled_dot_product_attention

P0
mx.fast.rms_norm

P1
mx.fast.rope / RoPE cache

P1
mx.compile inference

P1
compile cache

P2
static shape bucket

P2
DataFrame preprocessing
```

---

# 57. Production runtimeはMLX最適化と分離

本番環境では、

```text
MLX int4
```

をそのまま利用する必要はない。

local training終了後のportable safetensorsを入力として、

```text
Production Optimization Pipeline
```

を別途構築する。

---

# 58. Linux CPU production想定

本番がLinux CPUの場合、最低限以下を比較できる形式にする。

```text
PyTorch CPU
ONNX Runtime
OpenVINO
```

使用可能な環境に合わせてbenchmarkする。

---

# 59. Production model optimization

portable fp32/bf16 modelを基準として、

production側で、

```text
dynamic INT8
static INT8
weight-only INT8
```

等を実験する。

CPUでは、

```text
MLX int4
```

とは別の量子化方式を利用する。

---

# 60. 学習結果とproduction quantizationを分離

理想フロー:

```text
M5 Pro Max

Chronos-2 base
↓
MLX QLoRA
↓
training complete
↓
dequantize
↓
LoRA fuse
↓
standard bf16/fp32 safetensors
          │
          ▼
Linux Production
          │
          ├─ ONNX export
          ├─ OpenVINO conversion
          └─ CPU-specific INT8 quantization
```

これを基本設計とする。

---

# 61. なぜこの順番にするか

以下を避けるため。

```text
MLX量子化方式
↓
本番CPUでもそのまま利用

→ runtime非互換
→ 最適kernelを使えない
→ portability低下
```

学習用量子化とproduction量子化は別物として扱う。

---

# 62. Deployment validation

export後のモデルについて、

同一validation datasetで、

```text
MLX training model
standard exported model
production optimized model
```

を比較する。

---

# 63. Production許容誤差

3段階で確認。

```text
MLX fused
vs
portable model
```

はほぼparity。

```text
portable model
vs
CPU bf16/fp32
```

もほぼparity。

```text
portable
vs
INT8 production model
```

は精度劣化を測定して許容範囲内のみ採用。

---

# 64. 新しいTrainConfig案

概念的に以下へ拡張する。

```python
@dataclass
class TrainConfig:
    prediction_length: int = 24
    context_length: int = 512

    learning_rate: float = 1e-4
    weight_decay: float = 0.01

    batch_size: int = 32
    gradient_accumulation_steps: int = 1

    max_steps: int = 1000
    warmup_steps: int = 100

    lr_scheduler: str = "cosine"

    grad_clip: float = 1.0

    finetune_mode: str = "lora"

    dtype: str = "bfloat16"
    mixed_precision: bool = True

    compile: bool = True

    static_batch: bool = True
    drop_last: bool = True

    gradient_checkpointing: bool = False

    dataset_mode: str = "auto"

    lora: LoRAConfig = field(
        default_factory=LoRAConfig
    )
```

API名は既存コードスタイルに合わせて調整してよい。

---

# 65. 推奨実装ファイル構成

既存:

```text
train.py
```

が肥大化する場合は分割する。

例えば:

```text
src/chronos2_mlx/
├── train.py
├── training/
│   ├── dataloader.py
│   ├── step.py
│   ├── scheduler.py
│   ├── precision.py
│   └── checkpoint.py
├── export.py
└── benchmark_training.py
```

過剰な分割は不要。

---

# 66. テスト追加

最低限:

```text
tests/test_training_compile.py
tests/test_training_bf16.py
tests/test_gradient_accumulation.py
tests/test_qlora_freeze.py
tests/test_static_batch.py
tests/test_lr_scheduler.py
tests/test_export.py
tests/test_export_parity.py
```

---

# 67. Gradient Accumulation test

以下が近似一致すること。

```text
batch 32
accumulation 1
```

vs

```text
micro batch 8
accumulation 4
```

同一effective batch。

gradientとparameter updateを比較する。

---

# 68. QLoRA freeze test

training前後で、

```text
base quantized weights
scale
bias
```

が変化していないこと。

LoRAのみ変化すること。

---

# 69. Export test

MLXで学習:

```text
LoRA
```

↓

```text
fuse
```

↓

```text
export
```

↓

standard Chronos-2 loaderでload。

同一入力でpredictionを比較する。

---

# 70. benchmark-driven development

一度に全部実装してはいけない。

以下の順番とする。

```text
Baseline measurement

↓

fused SDPA

↓

bf16 training

↓

mx.compile training

↓

static shapes

↓

gradient accumulation

↓

DataLoader optimization

↓

QLoRA specialization

↓

optimizer memory optimization

↓

portable export

↓

production runtime optimization
```

各段階で、

```text
性能
メモリ
精度
```

を計測する。

---

# 71. 最優先P0

最初に実施。

```text
P0-1
Training baseline benchmark

P0-2
mx.fast.scaled_dot_product_attention

P0-3
bf16 mixed precision training

P0-4
mx.compile(loss + backward)

P0-5
static batch shape

P0-6
portable LoRA/QLoRA export
```

---

# 72. P1

```text
gradient accumulation
DataLoader MLX residency
QLoRA base freeze validation
LR warmup/scheduler
mx.fast.rms_norm
mx.fast.rope
checkpoint/resume
```

---

# 73. P2

```text
gradient checkpointing
optimizer state memory optimization
auto batch tuning
dataset streaming
prefetch
production CPU quantization
```

---

# 74. 採用基準

複雑な最適化は、

```text
>= 5%
```

程度の実測改善を原則採用ラインとする。

ただし、

```text
peak memory 20%以上減少
```

など明確なmemory benefitがある場合は採用可。

---

# 75. 最終レポート

以下を提出する。

## Training

```text
baseline steps/sec
optimized steps/sec
speedup
peak memory
validation loss
```

## QLoRA

```text
base memory
trainable params
optimizer state size
steps/sec
```

## Export

```text
MLX model
vs
portable safetensors
prediction error
```

## Production

```text
runtime
precision
latency
throughput
memory
accuracy delta
```

---

# 76. 最終成果物

必須:

1. M5 Pro Max向けoptimized training
2. bf16 mixed precision
3. training step `mx.compile`
4. gradient accumulation
5. static shape training
6. optimized DataLoader
7. QLoRA frozen-base path
8. optimizer state削減
9. checkpoint/resume
10. standard Chronos-2 safetensors export
11. LoRA fuse export
12. QLoRA dequantize + fuse export
13. MLX ↔ exported model parity test
14. production optimization用artifact
15. training benchmark
16. inference benchmark
17. README documentation

---

# 77. 最終的に実現したい利用方法

## Local training

```python
pipe = Chronos2MLXPipeline.from_pretrained(
    "amazon/chronos-2",
    dtype="bfloat16",
)

config = TrainConfig(
    finetune_mode="qlora",
    dtype="bfloat16",
    compile=True,
    batch_size=32,
    gradient_accumulation_steps=4,
    static_batch=True,
)

fine_tune(
    pipe.model,
    train_series,
    config,
)
```

---

## Portable export

理想API:

```python
export_pretrained(
    pipe.model,
    "./chronos2-custom",
    fuse_lora=True,
    dequantize=True,
    dtype="bfloat16",
)
```

成果物:

```text
chronos2-custom/
├── config.json
├── model.safetensors
└── export_metadata.json
```

---

## Production

Productionでは、

```text
./chronos2-custom
```

を入力として、

本番環境に最適なruntimeへ変換する。

MLXは要求しない。

---

# 78. 最重要方針

このプロジェクトの目的は、

```text
「MLXモデルを作る」
```

ことではない。

目的は、

```text
M5 Pro MaxのGPU / Unified Memoryを最大限利用して
Chronos-2を高速・省メモリに学習する

↓

その学習成果を標準Chronos-2モデルへ戻す

↓

本番環境では本番ハードウェアに最適化して実行する
```

ことである。

したがって、

```text
MLX-specific optimization
```

はtraining/runtime implementation detailとして扱う。

学習成果そのものにはMLX依存を残さないこと。

---

# 79. 最初に着手する作業

まず現在の `train.py` をbaselineとして、

以下4パターンをM5 Pro Max上で比較する。

```text
A:
fp32
compile off

B:
bf16
compile off

C:
bf16
compile on

D:
bf16
compile on
+
fused SDPA
```

測定:

```text
steps/sec
samples/sec
step latency
peak memory
validation loss
```

次に、

```text
gradient accumulation
```

を追加する。

その後、

```text
QLoRA
```

のbase freezeとmemory使用量を検証する。

最後に、

```text
standard safetensors export
```

を完成させ、

MLXで学習したモデルをstandard Chronos-2実装から読み込み、

同一入力に対するprediction parityを確認すること。

このexport parityが成立するまで「学習最適化完了」としないこと。
