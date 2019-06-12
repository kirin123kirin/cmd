#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import absolute_import
__doc__ = """
Summary: keras NN Library

Description: Very Easy Deeplearning API

:REQUIRES: Tensorflow, keras, numpy, pandas

"""

import numpy as np
from collections import defaultdict

from keras import backend as K
from keras import activations, initializers, regularizers, constraints
from keras.layers import Layer, InputSpec

from keras.utils.conv_utils import conv_output_length

from keras.layers.core import Dense, Flatten, Dropout
from keras.layers.convolutional import Conv1D
from keras.layers.pooling import MaxPooling1D
from keras.layers import Input
from keras.models import Model, Sequential, model_from_json
from keras.layers.recurrent import LSTM
import keras.backend.tensorflow_backend as KTF

import tensorflow as tf
try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None
import pandas as pd
import os
from functools import reduce

in_out_neurons = 1
hidden_neurons = 64
f_model = "./"
f_img = "./"


def _dropout(x, level, noise_shape=None, seed=None):
    x = K.dropout(x, level, noise_shape, seed)
    x *= (1. - level)  # compensate for the scaling by the dropout
    return x


def _categorical(y, num_classes=None, dtype='int32'):
    y = np.array(y, dtype='int')
    input_shape = y.shape
    if input_shape and input_shape[-1] == 1 and len(input_shape) > 1:
        input_shape = tuple(input_shape[:-1])
    y = y.ravel()
    if not num_classes:
        num_classes = np.max(y) + 1
    n = y.shape[0]
    categorical = np.zeros((n, num_classes), dtype=dtype)
    categorical[np.arange(n), y] = 1
    output_shape = input_shape + (num_classes,)
    categorical = np.reshape(categorical, output_shape)
    return categorical

def _force_array(arr):
    if isinstance(arr, np.ndarray):
        return arr
    if hasattr(arr, "__iter__") or hasattr(arr, "__next__"):
        return np.array(list(arr))
    return np.array(arr)

def _uniq(arr):
    arr = _force_array(arr)
    uniques, ids = np.unique(arr, return_inverse=True)
    if arr.shape == ids.shape:
        return uniques, ids
    else:
        return uniques, ids.reshape(arr.shape)

def onehot(tar, L=None):
    tar = _force_array(tar)
    try:
        return np.arange(L or tar.astype(int).max() + 1), _categorical(tar, L, int)
    except (ValueError, IndexError):
        uq, idx = _uniq(tar)
        return uq, np.eye(L or len(uq))[idx]

def hotone(uniq, idx, na_value=None):
    c = defaultdict(list)
    l = defaultdict(int)
    for x in np.argwhere(_force_array(idx)).tolist():
        k = tuple(x[:-1])
        c[k].append(x[-1])
        l[k] += 1
    
    if c:
        m = np.full([len(c), max(l.values())], -1)
        for i, v in enumerate(c.values()):
            
            if len(v) > 1:
                m[i, :len(v)] = v
            else:
                m[i] = v
        return np.append(_force_array(uniq), na_value)[m]

class QRNN(Layer):

    def __init__(self, units, window_size=2, stride=1,
                 return_sequences=False, go_backwards=False,
                 stateful=False, unroll=False, activation='tanh',
                 kernel_initializer='uniform', bias_initializer='zero',
                 kernel_regularizer=None, bias_regularizer=None,
                 activity_regularizer=None,
                 kernel_constraint=None, bias_constraint=None,
                 dropout=0, use_bias=True, input_dim=None, input_length=None,
                 **kwargs):
        self.return_sequences = return_sequences
        self.go_backwards = go_backwards
        self.stateful = stateful
        self.unroll = unroll

        self.units = units
        self.window_size = window_size
        self.strides = (stride, 1)

        self.use_bias = use_bias
        self.activation = activations.get(activation)
        self.kernel_initializer = initializers.get(kernel_initializer)
        self.bias_initializer = initializers.get(bias_initializer)
        self.kernel_regularizer = regularizers.get(kernel_regularizer)
        self.bias_regularizer = regularizers.get(bias_regularizer)
        self.activity_regularizer = regularizers.get(activity_regularizer)
        self.kernel_constraint = constraints.get(kernel_constraint)
        self.bias_constraint = constraints.get(bias_constraint)

        self.dropout = dropout
        self.supports_masking = True
        self.input_spec = [InputSpec(ndim=3)]
        self.input_dim = input_dim
        self.input_length = input_length
        if self.input_dim:
            kwargs['input_shape'] = (self.input_length, self.input_dim)
        super(QRNN, self).__init__(**kwargs)

    def build(self, input_shape):
        if isinstance(input_shape, list):
            input_shape = input_shape[0]

        batch_size = input_shape[0] if self.stateful else None
        self.input_dim = input_shape[2]
        self.input_spec = InputSpec(shape=(batch_size, None, self.input_dim))
        self.state_spec = InputSpec(shape=(batch_size, self.units))

        self.states = [None]
        if self.stateful:
            self.reset_states()

        kernel_shape = (self.window_size, 1, self.input_dim, self.units * 3)
        self.kernel = self.add_weight(name='kernel',
                                      shape=kernel_shape,
                                      initializer=self.kernel_initializer,
                                      regularizer=self.kernel_regularizer,
                                      constraint=self.kernel_constraint)
        if self.use_bias:
            self.bias = self.add_weight(name='bias',
                                        shape=(self.units * 3,),
                                        initializer=self.bias_initializer,
                                        regularizer=self.bias_regularizer,
                                        constraint=self.bias_constraint)

        self.built = True

    def compute_output_shape(self, input_shape):
        if isinstance(input_shape, list):
            input_shape = input_shape[0]

        length = input_shape[1]
        if length:
            length = conv_output_length(length + self.window_size - 1,
                                        self.window_size, 'valid',
                                        self.strides[0])
        if self.return_sequences:
            return (input_shape[0], length, self.units)
        else:
            return (input_shape[0], self.units)

    def compute_mask(self, inputs, mask):
        if self.return_sequences:
            return mask
        else:
            return None

    def get_initial_states(self, inputs):
        initial_state = K.zeros_like(inputs)  # (samples, timesteps, input_dim)
        initial_state = K.sum(initial_state, axis=(1, 2))  # (samples,)
        initial_state = K.expand_dims(initial_state)  # (samples, 1)
        initial_state = K.tile(initial_state, [1, self.units])  # (samples, units)
        initial_states = [initial_state for _ in range(len(self.states))]
        return initial_states

    def reset_states(self, states=None):
        if not self.stateful:
            raise AttributeError('Layer must be stateful.')
        if not self.input_spec:
            raise RuntimeError('Layer has never been called '
                               'and thus has no states.')

        batch_size = self.input_spec.shape[0]
        if not batch_size:
            raise ValueError('If a QRNN is stateful, it needs to know '
                             'its batch size. Specify the batch size '
                             'of your input tensors: \n'
                             '- If using a Sequential model, '
                             'specify the batch size by passing '
                             'a `batch_input_shape` '
                             'argument to your first layer.\n'
                             '- If using the functional API, specify '
                             'the time dimension by passing a '
                             '`batch_shape` argument to your Input layer.')

        if self.states[0] is None:
            self.states = [K.zeros((batch_size, self.units))
                           for _ in self.states]
        elif states is None:
            for state in self.states:
                K.set_value(state, np.zeros((batch_size, self.units)))
        else:
            if not isinstance(states, (list, tuple)):
                states = [states]
            if len(states) != len(self.states):
                raise ValueError('Layer ' + self.name + ' expects ' +
                                 str(len(self.states)) + ' states, '
                                 'but it received ' + str(len(states)) +
                                 'state values. Input received: ' +
                                 str(states))
            for index, (value, state) in enumerate(zip(states, self.states)):
                if value.shape != (batch_size, self.units):
                    raise ValueError('State ' + str(index) +
                                     ' is incompatible with layer ' +
                                     self.name + ': expected shape=' +
                                     str((batch_size, self.units)) +
                                     ', found shape=' + str(value.shape))
                K.set_value(state, value)

    def __call__(self, inputs, initial_state=None, **kwargs):
        # If `initial_state` is specified,
        # and if it a Keras tensor,
        # then add it to the inputs and temporarily
        # modify the input spec to include the state.
        if initial_state is not None:
            if hasattr(initial_state, '_keras_history'):
                # Compute the full input spec, including state
                input_spec = self.input_spec
                state_spec = self.state_spec
                if not isinstance(state_spec, list):
                    state_spec = [state_spec]
                self.input_spec = [input_spec] + state_spec

                # Compute the full inputs, including state
                if not isinstance(initial_state, (list, tuple)):
                    initial_state = [initial_state]
                inputs = [inputs] + list(initial_state)

                # Perform the call
                output = super(QRNN, self).__call__(inputs, **kwargs)

                # Restore original input spec
                self.input_spec = input_spec
                return output
            else:
                kwargs['initial_state'] = initial_state
        return super(QRNN, self).__call__(inputs, **kwargs)

    def call(self, inputs, mask=None, initial_state=None, training=None):
        # input shape: `(samples, time (padded with zeros), input_dim)`
        # note that the .build() method of subclasses MUST define
        # self.input_spec and self.state_spec with complete input shapes.
        if isinstance(inputs, list):
            initial_states = inputs[1:]
            inputs = inputs[0]
        elif initial_state is not None:
            pass
        elif self.stateful:
            initial_states = self.states
        else:
            initial_states = self.get_initial_states(inputs)

        if len(initial_states) != len(self.states):
            raise ValueError('Layer has ' + str(len(self.states)) +
                             ' states but was passed ' +
                             str(len(initial_states)) +
                             ' initial states.')
        input_shape = K.int_shape(inputs)
        if self.unroll and input_shape[1] is None:
            raise ValueError('Cannot unroll a RNN if the '
                             'time dimension is undefined. \n'
                             '- If using a Sequential model, '
                             'specify the time dimension by passing '
                             'an `input_shape` or `batch_input_shape` '
                             'argument to your first layer. If your '
                             'first layer is an Embedding, you can '
                             'also use the `input_length` argument.\n'
                             '- If using the functional API, specify '
                             'the time dimension by passing a `shape` '
                             'or `batch_shape` argument to your Input layer.')
        constants = self.get_constants(inputs, training=None)
        preprocessed_input = self.preprocess_input(inputs, training=None)

        last_output, outputs, states = K.rnn(self.step, preprocessed_input,
                                             initial_states,
                                             go_backwards=self.go_backwards,
                                             mask=mask,
                                             constants=constants,
                                             unroll=self.unroll,
                                             input_length=input_shape[1])
        if self.stateful:
            updates = []
            for i in range(len(states)):
                updates.append((self.states[i], states[i]))
            self.add_update(updates, inputs)

        # Properly set learning phase
        if 0 < self.dropout < 1:
            last_output._uses_learning_phase = True
            outputs._uses_learning_phase = True

        if self.return_sequences:
            return outputs
        else:
            return last_output

    def preprocess_input(self, inputs, training=None):
        if self.window_size > 1:
            inputs = K.temporal_padding(inputs, (self.window_size - 1, 0))
        inputs = K.expand_dims(inputs, 2)

        output = K.conv2d(inputs, self.kernel, strides=self.strides,
                          padding='valid',
                          data_format='channels_last')
        output = K.squeeze(output, 2)
        if self.use_bias:
            output = K.bias_add(output, self.bias, data_format='channels_last')

        if self.dropout is not None and 0. < self.dropout < 1.:
            z = output[:, :, :self.units]
            f = output[:, :, self.units:2 * self.units]
            o = output[:, :, 2 * self.units:]
            f = K.in_train_phase(1 - _dropout(1 - f, self.dropout), f, training=training)
            return K.concatenate([z, f, o], -1)
        else:
            return output

    def step(self, inputs, states):
        prev_output = states[0]

        z = inputs[:, :self.units]
        f = inputs[:, self.units:2 * self.units]
        o = inputs[:, 2 * self.units:]

        z = self.activation(z)
        f = f if self.dropout is not None and 0. < self.dropout < 1. else K.sigmoid(f)
        o = K.sigmoid(o)

        ct = f * prev_output + (1 - f) * z
        output = o * ct
        return output, [ct]

    def get_constants(self, inputs, training=None):
        return []

    def get_config(self):
        config = {'units': self.units,
                  'window_size': self.window_size,
                  'stride': self.strides[0],
                  'return_sequences': self.return_sequences,
                  'go_backwards': self.go_backwards,
                  'stateful': self.stateful,
                  'unroll': self.unroll,
                  'use_bias': self.use_bias,
                  'dropout': self.dropout,
                  'activation': activations.serialize(self.activation),
                  'kernel_initializer': initializers.serialize(self.kernel_initializer),
                  'bias_initializer': initializers.serialize(self.bias_initializer),
                  'kernel_regularizer': regularizers.serialize(self.kernel_regularizer),
                  'bias_regularizer': regularizers.serialize(self.bias_regularizer),
                  'activity_regularizer': regularizers.serialize(self.activity_regularizer),
                  'kernel_constraint': constraints.serialize(self.kernel_constraint),
                  'bias_constraint': constraints.serialize(self.bias_constraint),
                  'input_dim': self.input_dim,
                  'input_length': self.input_length}
        base_config = super(QRNN, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))


class SequentialModel(object):

    def __init__(self, model_type, x_train, y_train, x_test=None, y_test=None):

        self.old_session = KTF.get_session()
        self.session = tf.Session('')
        KTF.set_session(self.session)
        self.model_type = model_type

        if isinstance(model_type, Model):
            self.model = model_type

        elif model_type == "nn":
            l_seq = x_train.shape[1:] if len(x_train.shape) > 1 else  (1, )
            r_seq = reduce(lambda a,b: a*b, y_train.shape[1:]) or 1

            self.model = create_nn_model(l_seq, r_seq)

        else:
            if len(x_train.shape) < 3:
                x_train = x_train.reshape(x_train.shape + (1, ))
                x_test = x_test.reshape(x_test.shape + (1, )) if x_test else None

            l_seq = x_train.shape[1:] if len(x_train.shape) > 1 else  (1, )
            r_seq = reduce(lambda a,b: a*b, y_train.shape[1:]) or 1

            if model_type == "cnn":
                self.model = create_cnn_model(l_seq, r_seq)
            elif model_type == "rnn":
                self.model = create_rnn_model(l_seq, r_seq)
            elif model_type == "qrnn":
                self.model = create_qrnn_model(l_seq, r_seq)
            else:
                raise ValueError("Unknown model type")

        self.x_train = x_train
        self.y_train = y_train
        self.x_test = x_test
        self.y_test = y_test
        self.history = None

    def save(self, path=os.path.join(f_model, 'pred.json')):
        json_string = self.model.to_json()
        open(path, 'w').write(json_string)
        print("saved... " + path)
        wpath = os.path.splitext(path)[0] + '_weights.hdf5'
        self.model.save_weights(wpath)
        print("saved... " + wpath)

    def fit(self, epochs, x_train=None, y_train=None, x_test=None, y_test=None, batch_size=20, validation_split=0.1, verbose=1):
        self.history = self.model.fit(x_train or self.x_train,
                       y_train or self.y_train,
                       batch_size=batch_size,
                       epochs=epochs,
                       validation_split= 0.0 if x_test and y_test else validation_split,
                       validation_data=(x_test, y_test) if x_test and y_test else None,
                       verbose=verbose,
                       )
        return self.history

    def predict(self, test=None):
        return self.model.predict(test or self.x_test or self.x_train[-100:])

    def sequential_predict(self, dataf, l_seq, start=0):
        l_pred = 350
        now = dataf.iloc[start:start + l_seq].as_matrix()
        df = pd.DataFrame(dataf.iloc[start + l_seq - 150: start + l_seq + l_pred].as_matrix())
        df.columns = ["true_value(observed_value)"]
        pred = []
        for i in range(l_pred):
            p = self.model.predict(np.array([now]))
            pred.append(p[0][0])
            now = np.roll(now, -1)
            now[-1] = pred[-1]
        df["predict"] = [None] * 150 + pred
        df.plot()
        plt.savefig(os.path.join(f_img, 'pred_2.png'))

    def show(self):
        """
        loss, accuracyのグラフ出す
        matplot要るよ、GUI環境要るよ
        """
        # 精度の履歴をプロット
        plt.plot(self.history.history['acc'],"o-",label="accuracy")
        if 'val_acc' in self.history.history:
            plt.plot(self.history.history['val_acc'],"o-",label="val_acc")
        plt.title('model accuracy')
        plt.xlabel('epoch')
        plt.ylabel('accuracy')
        plt.legend(loc="lower right")
        plt.show()

        # 損失の履歴をプロット
        plt.plot(self.history.history['loss'],"o-",label="loss",)
        if 'val_loss' in self.history.history:
            plt.plot(self.history.history['val_loss'],"o-",label="val_loss")
        plt.title('model loss')
        plt.xlabel('epoch')
        plt.ylabel('loss')
        plt.legend(loc='lower right')
        plt.show()

def create_nn_model(l_seq, r_seq=in_out_neurons, loss = 'mean_squared_error', optimizer = 'sgd', metrics=["accuracy"]):
    model = Sequential([
        Dense( units = 32, input_shape = (l_seq, ), activation='sigmoid' ),
        Dropout(0.1),
        Dense( units = 64, input_shape = (l_seq, ), activation='sigmoid' ),
        Dropout(0.1),
        Dense( units = 32, input_shape = (l_seq, ), activation='sigmoid' ),
        Dropout(0.1),
        Dense( units = r_seq , activation='softmax'),
    ])
    model.compile( loss = loss, optimizer = optimizer, metrics = metrics)
    return model

def create_qrnn_model(l_seq, r_seq=in_out_neurons, loss="mean_squared_error", optimizer="adam"):
    input_layer = Input(shape=l_seq)
    qrnn_output_layer = QRNN(64, window_size=60, dropout=0)(input_layer)
    prediction_result = Dense(r_seq)(qrnn_output_layer)
    model = Model(input=input_layer, output=prediction_result)
    model.compile(loss=loss, optimizer=optimizer, metrics=["accuracy"])
    return model

def create_rnn_model(l_seq, r_seq=in_out_neurons, loss="mean_squared_error", optimizer="rmsprop", hidden_neurons=300):
    inputs = Input(shape=l_seq)
    x = LSTM(hidden_neurons, return_sequences=False)(inputs)
    predictions = Dense(r_seq, activation='linear')(x)
    model = Model(input=inputs, output=predictions)
    model.compile(loss=loss, optimizer=optimizer, metrics=["accuracy"])
    return model

def create_cnn_model(l_seq, r_seq=in_out_neurons, loss="mean_squared_error", optimizer="adam", pool_size=5):
    inputs = Input(shape=l_seq)
    x = Conv1D(32,  3, activation='relu', padding='valid')(inputs)
    x = Conv1D(32,  3, activation='relu', padding='valid')(x)
    x = MaxPooling1D(pool_size=pool_size)(x)
    x = Conv1D(64,  3, activation='relu', padding='valid')(inputs)
    x = Conv1D(64,  3, activation='relu', padding='valid')(x)
    x = MaxPooling1D(pool_size=pool_size)(x)
    x = Flatten()(x)
    x = Dense(128, activation='relu')(x)
    x = Dense(64, activation='relu')(x)
    predictions = Dense(r_seq, activation='linear')(x)
    model = Model(input=inputs, output=predictions)
    model.compile(loss=loss, optimizer=optimizer, metrics=["accuracy"])
    return model

def run(epochs, x_train, y_train, x_test=None, y_test=None, model_type="qrnn", batch_size=20, prediction_start_index=0, load=False, validation_split=0.1, verbose=1):
    model = SequentialModel(model_type, x_train, y_train, x_test, y_test)
    model.model.summary()
    if load:
        model.load()
    else:
        model.fit(int(epochs), batch_size=int(batch_size), validation_split=validation_split, verbose=verbose)
    model.save()
    if plt:
        model.show()
    return model, model.predict(x_test)

def load(json_path="pred.json", hd5_path="pred_weights.hdf5"):
    with open(json_path) as f:
        model = model_from_json(f.read(), {"QRNN":QRNN})
        model.load_weights(hd5_path)
        return model
