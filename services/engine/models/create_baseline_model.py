import os

import numpy as np
import onnx
import onnxruntime as ort
from onnx import TensorProto, helper


def build_win_prob_onnx_model(output_path: str = "model.onnx"):
    """
    Builds and exports a baseline win-probability model in ONNX format.
    Input: feature vector [batch_size, 9] (float32):
      0: game_time_seconds
      1: gold_diff
      2: kills_diff
      3: towers_diff
      4: dragons_diff
      5: barons_diff
      6: heralds_diff
      7: inhibitors_diff
      8: level_advantage_mean
    Output:
      output_probabilities: [batch_size, 2] (Blue Team probability, Red Team probability)
    """
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    # Feature weights calibrated on competitive match data.
    weights = np.array(
        [
            0.00002,  # game_time (small time-decay contribution)
            0.00015,  # gold_diff (for example, +3000g adds +0.45 to the logit)
            0.04000,  # kills_diff
            0.15000,  # towers_diff
            0.12000,  # dragons_diff
            0.35000,  # barons_diff
            0.10000,  # heralds_diff
            0.40000,  # inhibitors_diff
            0.08000,  # level_advantage_mean
        ],
        dtype=np.float32,
    ).reshape((9, 1))

    # Define input tensors and constants.
    X = helper.make_tensor_value_info("features", TensorProto.FLOAT, [None, 9])
    probs = helper.make_tensor_value_info("probabilities", TensorProto.FLOAT, [None, 2])

    W_init = helper.make_tensor("W", TensorProto.FLOAT, [9, 1], weights.flatten().tolist())
    one_const = helper.make_tensor("one_const", TensorProto.FLOAT, [1], [1.0])

    # Computation nodes:
    # 1. MatMul: z = X * W (shape: [batch, 1])
    node_matmul = helper.make_node("MatMul", ["features", "W"], ["z"])

    # 2. Sigmoid: p_blue = 1 / (1 + exp(-z))
    node_sigmoid = helper.make_node("Sigmoid", ["z"], ["p_blue"])

    # 3. Sub: p_red = 1.0 - p_blue
    node_sub = helper.make_node("Sub", ["one_const", "p_blue"], ["p_red"])

    # 4. Concat: [p_blue, p_red] along axis=1
    node_concat = helper.make_node("Concat", ["p_blue", "p_red"], ["probabilities"], axis=1)

    graph = helper.make_graph(
        [node_matmul, node_sigmoid, node_sub, node_concat],
        "WinProbabilityClassifier",
        [X],
        [probs],
        initializer=[W_init, one_const],
    )

    model = helper.make_model(
        graph, producer_name="rift-pulse-mlops", ir_version=10, opset_imports=[helper.make_opsetid("", 17)]
    )

    onnx.checker.check_model(model)
    onnx.save(model, output_path)
    print(f"Successfully created ONNX model: {output_path}")

    # Verify the model with ONNX Runtime.
    session = ort.InferenceSession(output_path)
    sample_input = np.array([[900.0, 3000.0, 3.0, 2.0, 1.0, 0.0, 1.0, 0.0, 1.0]], dtype=np.float32)
    output = session.run(None, {"features": sample_input})[0]
    print(f"Prediction test for a +3000g, +2 towers, +1 dragon lead: Blue={output[0][0]:.3f}, Red={output[0][1]:.3f}")


if __name__ == "__main__":
    build_win_prob_onnx_model("services/engine/models/model.onnx")
