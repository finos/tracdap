#  Licensed to the Fintech Open Source Foundation (FINOS) under one or
#  more contributor license agreements. See the NOTICE file distributed
#  with this work for additional information regarding copyright ownership.
#  FINOS licenses this file to you under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import inspect
import logging
import re
import types
import typing as tp
import pathlib

import tracdap.rt.metadata as meta
import tracdap.rt.exceptions as ex
import tracdap.rt._impl.core.logging as log
import tracdap.rt._impl.core.util as util

# _Named placeholder type from API hook is needed for API type checking
from tracdap.rt.api.hook import _Named  # noqa


def require_package(module_name: str, module_obj: types.ModuleType):
    if module_obj is None:
        raise ex.ERuntimeValidation(f"Optional package [{module_name}] is not installed")


def validate_signature(method: tp.Callable, *args, **kwargs):
    _TypeValidator.validate_signature(method, *args, **kwargs)


def validate_return_type(method: tp.Callable, value: tp.Any):
    _TypeValidator.validate_return_type(method, value)


def check_type(expected_type: tp.Type, value: tp.Any) -> bool:
    return _TypeValidator.check_type(expected_type, value)


def type_name(type_: tp.Type, qualified: bool) -> str:
    return _TypeValidator._type_name(type_, qualified)  # noqa


def quick_validate_model_def(model_def: meta.ModelDefinition):
    StaticValidator.quick_validate_model_def(model_def)


def is_primitive_type(basic_type: meta.BasicType) -> bool:
    return StaticValidator.is_primitive_type(basic_type)


T_SKIP_VAL = tp.TypeVar("T_SKIP_VAL")

class SkipValidation(tp.Generic[T_SKIP_VAL]):
    def __init__(self, skip_type: tp.Type[T_SKIP_VAL]):
        self.skip_type = skip_type


class _TypeValidator:

    # Support both new and old styles for generic, union and optional types
    # Old-style annotations are still valid, even when the new style is fully supported
    __generic_metaclass = [
        types.GenericAlias,
        type(tp.List[object]),
        type(tp.Optional[object])
    ]

    # UnionType was added to the types module in Python 3.10, we support 3.9 (Jan 2025)
    if hasattr(types, "UnionType"):
        __generic_metaclass.append(types.UnionType)

    # Cache method signatures to avoid inspection on every call
    # Inspecting a function signature can take ~ half a second in Python 3.7
    __method_cache: tp.Dict[str, tp.Tuple[inspect.Signature, tp.Any]] = dict()

    _log: logging.Logger = log.logger_for_namespace(__name__)

    @classmethod
    def validate_signature(cls, method: tp.Callable, *args, **kwargs):

        if method.__qualname__ in cls.__method_cache:
            signature, hints = cls.__method_cache[method.__qualname__]
        else:
            signature = inspect.signature(method)
            hints = tp.get_type_hints(method)
            cls.__method_cache[method.__qualname__] = signature, hints

        named_params = list(signature.parameters.keys())
        positional_index = 0

        for param_name, param in signature.parameters.items():

            param_type = hints.get(param_name)

            values = cls._select_arg(method.__name__, param, positional_index, named_params, *args, **kwargs)
            positional_index += len(values)

            for value in values:
                cls._validate_arg(method.__name__, param_name, param_type, value)

    @classmethod
    def validate_return_type(cls, method: tp.Callable, value: tp.Any):

        if method.__qualname__ in cls.__method_cache:
            signature, hints = cls.__method_cache[method.__qualname__]
        else:
            signature = inspect.signature(method)
            hints = tp.get_type_hints(method)
            cls.__method_cache[method.__qualname__] = signature, hints

        correct_type = cls._validate_type(signature.return_annotation, value)

        if not correct_type:
            err = f"Invalid API return type for [{method.__name__}()]: " + \
                  f"Expected [{signature.return_annotation}], got [{type(value)}]"
            cls._log.error(err)
            raise ex.ERuntimeValidation(err)

    @classmethod
    def check_type(cls, expected_type: tp.Type, value: tp.Any) -> bool:

        return cls._validate_type(expected_type, value)

    @classmethod
    def _select_arg(
            cls, method_name: str, parameter: inspect.Parameter, positional_index, named_params,
            *args, **kwargs) -> tp.List[tp.Any]:

        if parameter.kind == inspect.Parameter.POSITIONAL_ONLY:

            if positional_index < len(args):
                return [args[positional_index]]

            else:
                err = f"Invalid API call [{method_name}()]: Missing required parameter [{parameter.name}]"
                cls._log.error(err)
                raise ex.ERuntimeValidation(err)

        if parameter.kind == inspect.Parameter.KEYWORD_ONLY:

            if parameter.name in kwargs:
                return [kwargs[parameter.name]]

            else:
                err = f"Invalid API call [{method_name}()]: Missing required parameter [{parameter.name}]"
                cls._log.error(err)
                raise ex.ERuntimeValidation(err)

        if parameter.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:

            if positional_index < len(args):
                return [args[positional_index]]

            elif parameter.name in kwargs:
                return [kwargs[parameter.name]]

            else:
                err = f"Invalid API call [{method_name}()]: Missing required parameter [{parameter.name}]"
                cls._log.error(err)
                raise ex.ERuntimeValidation(err)

        if parameter.kind == inspect.Parameter.VAR_POSITIONAL:

            if positional_index < len(args):
                return [args[i] for i in range(positional_index, len(args))]
            else:
                return []

        if parameter.kind == inspect.Parameter.VAR_KEYWORD:

            return [arg for kw, arg in kwargs.items() if kw not in named_params]

        raise ex.EUnexpected("Invalid method signature in runtime API (this is a bug)")

    @classmethod
    def _validate_arg(cls, method_name: str, param_name: str, param_type: tp.Type, value: tp.Any):

        if not cls._validate_type(param_type, value):

            expected_type = cls._type_name(param_type)
            actual_type = cls._type_name(type(value)) if value is not None else str(None)

            if expected_type == actual_type:
                expected_type = cls._type_name(param_type, qualified=True)
                actual_type = cls._type_name(type(value), qualified=True)

            err = f"Invalid API call [{method_name}()]: Wrong type for [{param_name}]" \
                  + f" (expected [{expected_type}], got [{actual_type}])"

            cls._log.error(err)
            raise ex.ERuntimeValidation(err)

    @classmethod
    def _validate_type(cls, expected_type: tp.Type, value: tp.Any) -> bool:

        if expected_type == tp.Any:
            return True

        # Sometimes we need to validate a partial set of arguments
        # Explicitly passing a SkipValidation value allows for this
        if isinstance(value, SkipValidation):
            if value.skip_type == expected_type:
                return True

        if any(map(lambda _t: isinstance(expected_type, _t),  cls.__generic_metaclass)):

            origin = util.get_origin(expected_type)
            args = util.get_args(expected_type)

            # The generic type "_Named" is defined in the TRAC API so needs to be supported
            # This type is used for passing named intermediate values between the define_ methods
            if origin is _Named:

                named_type = args[0]
                return isinstance(value, _Named) and cls._validate_type(named_type, value.item)

            # _tp.Union also covers _tp.Optional, which is shorthand for _tp.Union[_type, None]
            if origin is tp.Union:

                for union_type in args:
                    if cls._validate_type(union_type, value):
                        return True

                return False

            if origin is list:

                list_type = args[0]
                return isinstance(value, list) and \
                    all(map(lambda v: cls._validate_type(list_type, v), value))

            if origin is dict:

                key_type = args[0]
                value_type = args[1]

                return isinstance(value, dict) and \
                    all(map(lambda k: cls._validate_type(key_type, k), value.keys())) and \
                    all(map(lambda v: cls._validate_type(value_type, v), value.values()))

            if origin is type:

                if not isinstance(value, type):
                    return False

                type_arg = args[0]

                if type_arg == tp.Any:
                    return True

                if isinstance(type_arg, tp.TypeVar):

                    constraints =  util.get_constraints(type_arg)
                    bound = util.get_bound(type_arg)

                    if constraints:
                        if not any(map(lambda c: expected_type == c, constraints)):
                            return False

                    if bound:
                        if not issubclass(expected_type, bound):
                            return False

                    # So long as constraints / bound are ok, any type matches a generic type var
                    return True


            if origin.__module__.startswith("tracdap.rt.api."):
                return isinstance(value, origin)

            raise ex.ETracInternal(f"Validation of [{origin.__name__}] generic parameters is not supported yet")

        # Support for generic type variables
        if isinstance(expected_type, tp.TypeVar):

            # If there are any constraints or a bound, those must be honoured

            constraints =  util.get_constraints(expected_type)
            bound = util.get_bound(expected_type)

            if constraints:
                if not any(map(lambda c: type(value) == c, constraints)):
                    return False

            if bound:
                if not isinstance(value, bound):
                    return False

            # So long as constraints / bound are ok, any type matches a generic type var
            return True


        # Validate everything else as a concrete type

        # TODO: Recursive validation of types for class members using field annotations

        return isinstance(value, expected_type)

    @classmethod
    def _type_name(cls, type_var: tp.Type, qualified: bool = False) -> str:

        if any(map(lambda _t: isinstance(type_var, _t),  cls.__generic_metaclass)):

            origin = util.get_origin(type_var)
            args = util.get_args(type_var)

            if origin is _Named:
                named_type = cls._type_name(args[0])
                return f"Named[{named_type}]"

            if origin is tp.Union:
                if len(args) == 2 and args[1] == type(None):
                    return f"Optional[{cls._type_name(args[0])}]"
                else:
                    return "|".join(map(cls._type_name, args))

            if origin is list:
                list_type = cls._type_name(args[0])
                return f"list[{list_type}]"

            if origin is type:
                type_arg = cls._type_name(args[0])
                return f"type[{type_arg}]"

            raise ex.ETracInternal(f"Validation of [{origin.__name__}] generic parameters is not supported yet")

        if qualified:
            return f"{type_var.__module__}.{type_var.__name__}"
        else:
            return type_var.__name__


class StaticValidator:

    __identifier_pattern = re.compile("\\A[a-zA-Z_]\\w*\\Z", re.ASCII)
    __reserved_identifier_pattern = re.compile("\\A(_|trac_)", re.ASCII)
    __label_length_limit = 4096

    __file_extension_pattern = re.compile('\\A[a-zA-Z0-9]+\\Z')
    __mime_type_pattern = re.compile('\\A\\w+/[-.\\w]+(?:\\+[-.\\w]+)?\\Z')

    __PRIMITIVE_TYPES = [
        meta.BasicType.BOOLEAN,
        meta.BasicType.INTEGER,
        meta.BasicType.FLOAT,
        meta.BasicType.DECIMAL,
        meta.BasicType.STRING,
        meta.BasicType.DATE,
        meta.BasicType.DATETIME,
    ]

    __BUSINESS_KEY_TYPES = [
        meta.BasicType.STRING,
        meta.BasicType.INTEGER,
        meta.BasicType.DATE]

    _log: logging.Logger = log.logger_for_namespace(__name__)

    @classmethod
    def is_primitive_type(cls, basic_type: meta.BasicType) -> bool:

        return basic_type in cls.__PRIMITIVE_TYPES

    @classmethod
    def quick_validate_model_def(cls, model_def: meta.ModelDefinition):

        # This is a quick validation that only checks for valid unique identifiers
        # Other checks, e.g. table field type is primitive, are not included yet

        # Note: This method must raise EModelValidation on failure, rather than any other validation error type
        # This will be important when comprehensive metadata validation is added to the runtime
        # If the standard static validator is used, validation errors must be caught and re-raised as EModelValidation

        attrs_type_check = _TypeValidator.check_type(tp.Dict[str, meta.Value], model_def.staticAttributes)
        params_type_check = _TypeValidator.check_type(tp.Dict[str, meta.ModelParameter], model_def.parameters)
        inputs_type_check = _TypeValidator.check_type(tp.Dict[str, meta.ModelInputSchema], model_def.inputs)
        outputs_type_check = _TypeValidator.check_type(tp.Dict[str, meta.ModelOutputSchema], model_def.outputs)

        if not attrs_type_check:
            cls._fail(f"Invalid model attributes: define_attributes() returned the wrong type")
        if not params_type_check:
            cls._fail(f"Invalid model parameters: define_parameters() returned the wrong type")
        if not inputs_type_check:
            cls._fail(f"Invalid model inputs: define_inputs() returned the wrong type")
        if not outputs_type_check:
            cls._fail(f"Invalid model outputs: define_outputs() returned the wrong type")

        cls._valid_identifiers(model_def.staticAttributes.keys(), "model attribute")
        cls._valid_identifiers(model_def.parameters.keys(), "model parameter")
        cls._valid_identifiers(model_def.inputs.keys(), "model input")
        cls._valid_identifiers(model_def.outputs.keys(), "model output")

        cls._case_insensitive_duplicates(model_def.staticAttributes.keys(), "model attribute")
        cls._case_insensitive_duplicates(model_def.parameters.keys(), "model parameter")
        cls._case_insensitive_duplicates(model_def.inputs.keys(), "model input")
        cls._case_insensitive_duplicates(model_def.outputs.keys(), "model output")

        # Note unique context does not include static attributes
        # They are not part of the runtime context, so there is no need to constrain them
        unique_ctx = {}
        cls._unique_context_check(unique_ctx, model_def.parameters.keys(), "model parameter")
        cls._unique_context_check(unique_ctx, model_def.inputs.keys(), "model input")
        cls._unique_context_check(unique_ctx, model_def.outputs.keys(), "model output")

        cls._check_parameters(model_def.parameters)
        cls._check_inputs_or_outputs(model_def.inputs)
        cls._check_inputs_or_outputs(model_def.outputs)

    @classmethod
    def quick_validate_schema(cls, schema: meta.SchemaDefinition):

        if schema.schemaType != meta.SchemaType.TABLE:
            cls._fail(f"Unsupported schema type [{schema.schemaType}]")

        if schema.partType != meta.PartType.PART_ROOT:
            cls._fail(f"Unsupported partition type [{schema.partType}]")

        if schema.table is None or schema.table.fields is None or len(schema.table.fields) == 0:
            cls._fail(f"Table schema does not define any fields")

        fields = schema.table.fields
        field_names = list(map(lambda f: f.fieldName, fields))
        property_type = f"field"

        cls._valid_identifiers(field_names, property_type)
        cls._case_insensitive_duplicates(field_names, property_type)

        for field in fields:
            cls._check_single_field(field, property_type)

    @classmethod
    def _check_label(cls, label, param_name):
        if label is not None:
            if len(label) > cls.__label_length_limit:
                cls._fail(f"Invalid model parameter: [{param_name}] label exceeds maximum length limit "
                          f"({cls.__label_length_limit} characters)")
            if len(label.strip()) == 0:
                cls._fail(f"Invalid model parameter: [{param_name}] label is blank")

    @classmethod
    def _check_parameters(cls, parameters):

        for param_name, param in parameters.items():

            if param.label is None:
                cls._fail(f"Invalid model parameter: [{param_name}] label is missing")
            else:
                cls._check_label(param.label, param_name)

            if param.paramProps is not None:
                cls._valid_identifiers(param.paramProps.keys(), "entry in param props")

    @classmethod
    def _check_inputs_or_outputs(cls, sockets):

        for socket_name, socket in sockets.items():

            if socket.objectType == meta.ObjectType.DATA:
                cls._check_socket_schema(socket_name, socket)
            elif socket.objectType == meta.ObjectType.FILE:
                cls._check_socket_file_type(socket_name, socket)
            else:
                raise ex.EModelValidation(f"Invalid object type [{socket.objectType.name}] for [{socket_name}]")

            label = socket.label
            cls._check_label(label, socket_name)

            if isinstance(socket, meta.ModelInputSchema):
                if socket.inputProps is not None:
                    cls._valid_identifiers(socket.inputProps.keys(), "entry in input props")
            else:
                if socket.outputProps is not None:
                    cls._valid_identifiers(socket.outputProps.keys(), "entry in output props")

    @classmethod
    def _check_socket_schema(cls, socket_name, socket):

        if socket.schema is None:
            cls._fail(f"Missing schema requirement for [{socket_name}]")
            return

        if socket.dynamic:
            if socket.schema and socket.schema.table:
                error = "Dynamic schemas must have schema.table = None"
                cls._fail(f"Invalid schema for [{socket_name}]: {error}")
            else:
                return

        if socket.schema.schemaType == meta.SchemaType.TABLE:

            fields = socket.schema.table.fields
            field_names = list(map(lambda f: f.fieldName, fields))
            property_type = f"field in [{socket_name}]"

            if len(fields) == 0:
                cls._fail(f"Invalid schema for [{socket_name}]: No fields defined")

            cls._valid_identifiers(field_names, property_type)
            cls._case_insensitive_duplicates(field_names, property_type)

            for field in fields:
                cls._check_single_field(field, property_type)

    @classmethod
    def _check_socket_file_type(cls, socket_name, socket):

        if socket.fileType is None:
            cls._fail(f"Missing file type requirement for [{socket_name}]")
            return

        if not cls.__file_extension_pattern.match(socket.fileType.extension):
            cls._fail(f"Invalid extension [{socket.fileType.extension}] for [{socket_name}]")

        if not cls.__mime_type_pattern.match(socket.fileType.mimeType):
            cls._fail(f"Invalid mime type [{socket.fileType.mimeType}] for [{socket_name}]")

    @classmethod
    def _check_single_field(cls, field: meta.FieldSchema, property_type):

        # Valid identifier and not trac reserved checked separately

        if field.fieldOrder < 0:
            cls._fail(f"Invalid {property_type}: [{field.fieldName}] fieldOrder < 0")

        if field.fieldType not in cls.__PRIMITIVE_TYPES:
            cls._fail(f"Invalid {property_type}: [{field.fieldName}] fieldType is not a primitive type")

        if field.label is None or len(field.label.strip()) == 0:
            cls._fail(f"Invalid {property_type}: [{field.fieldName}] label is missing or blank")

        if len(field.label) > cls.__label_length_limit:
            cls._fail(f"Invalid {property_type}: [{field.fieldName}] label exceeds maximum length limit ({cls.__label_length_limit} characters)")  # noqa

        if field.businessKey and field.fieldType not in cls.__BUSINESS_KEY_TYPES:
            cls._fail(f"Invalid {property_type}: [{field.fieldName}] fieldType {field.fieldType} used as business key")

        if field.categorical and field.fieldType != meta.BasicType.STRING:
            cls._fail(f"Invalid {property_type}: [{field.fieldName}] fieldType {field.fieldType} used as categorical")

        # Do not require notNull = True for business keys here
        # Instead setting businessKey = True will cause notNull = True to be set during normalization
        # This agrees with the semantics in platform API and CSV schema loader

    @classmethod
    def _valid_identifiers(cls, keys, property_type):

        for key in keys:
            if not cls.__identifier_pattern.match(key):
                cls._fail(f"Invalid {property_type}: [{key}] is not a valid identifier")
            if cls.__reserved_identifier_pattern.match(key):
                cls._fail(f"Invalid {property_type}: [{key}] is a reserved identifier")

    @classmethod
    def _case_insensitive_duplicates(cls, items, property_type):

        known_items = {}

        for item_case in items:

            lower_case = item_case.lower()
            prior_case = known_items.get(lower_case)

            if prior_case is None:
                known_items[lower_case] = item_case

            elif item_case == prior_case:
                err = f"[{item_case}] is included more than once as a {property_type}"
                cls._fail(err)

            else:
                err = f"[{prior_case}] and [{item_case}] are both included as a {property_type} but differ only by case"
                cls._fail(err)

    @classmethod
    def _unique_context_check(cls, unique_ctx, items, property_type):

        for item in items:

            lower_item = item.lower()

            if lower_item in unique_ctx:
                err = f"Model {property_type} [{item}] is already defined as a {unique_ctx[lower_item]}"
                cls._fail(err)

            unique_ctx[lower_item] = property_type

    @classmethod
    def _fail(cls, message: str):
        cls._log.error(message)
        raise ex.EModelValidation(message)


class StorageValidator:

    __ILLEGAL_PATH_CHARS_WINDOWS = re.compile(r".*[\x00<>:\"\'|?*].*")
    __ILLEGAL_PATH_CHARS_POSIX = re.compile(r".*[\x00<>:\"\'|?*\\].*")
    __ILLEGAL_PATH_CHARS = __ILLEGAL_PATH_CHARS_WINDOWS if util.is_windows() else __ILLEGAL_PATH_CHARS_POSIX

    @classmethod
    def storage_path_is_empty(cls, storage_path: str):

        return storage_path is None or len(storage_path.strip()) == 0

    @classmethod
    def storage_path_invalid(cls, storage_path: str):

        if cls.__ILLEGAL_PATH_CHARS.match(storage_path):
            return True

        try:
            # Make sure the path can be interpreted as a path
            pathlib.Path(storage_path)
            return False
        except ValueError:
            return True

    @classmethod
    def storage_path_not_relative(cls, storage_path: str):

        relative_path = pathlib.Path(storage_path)
        return relative_path.is_absolute()

    @classmethod
    def storage_path_outside_root(cls, storage_path: str):

        # is_relative_to only supported in Python 3.9+, we need to support 3.8

        root_path = pathlib.Path("C:\\root") if util.is_windows() else pathlib.Path("/root")
        relative_path = pathlib.Path(storage_path)
        absolute_path = root_path.joinpath(relative_path).resolve(False)

        return root_path != absolute_path and root_path not in absolute_path.parents

    @classmethod
    def storage_path_is_root(cls, storage_path: str):

        root_path = pathlib.Path("C:\\root") if util.is_windows() else pathlib.Path("/root")
        relative_path = pathlib.Path(storage_path)
        absolute_path = root_path.joinpath(relative_path).resolve(False)

        return root_path == absolute_path
