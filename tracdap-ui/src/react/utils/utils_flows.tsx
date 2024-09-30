import {convertArrayToOptions, countItemsInArray, makeArrayOfObjectsUniqueByProperty} from "./utils_arrays";
import {convertKeyToText} from "./utils_string";
import {createBlankTracGroup, getGroupOptionIndex} from "./utils_general";
import {differencesInSchemas} from "./utils_schema";
import {extractValueFromTracValueObject, getObjectName} from "./utils_trac_metadata";
import {hasOwnProperty, isDefined, isGroupOption, isObject, isOption, isStringArray, isTagOption} from "./utils_trac_type_chckers";
import {
    FlowGroup,
    JobInputsByCategory,
    GenericGroup,
    GregsEdge,
    NodeDetails,
    Option,
    RenamedDataset,
    SearchOption,
    TracGroup,
    ValidationOfModelInputs,
    ValidationOfModelsAndFlowData,
    ValidationOfModelsParameters
} from "../../types/types_general";
import {Models, Validation as FlowValidation} from "../scenes/RunAFlowScene/store/runAFlowStore";
import {Model, Validation as ModelValidation} from "../scenes/RunAModelScene/store/runAModelStore";
import {SingleValue} from "react-select";
import {UiAttributesProps} from "../../types/types_attributes_and_parameters";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * A function that extracts the input nodes from a flow.
 * @param flow - The flow to get the input nodes from.
 * @returns The input nodes.
 */
export const getFlowInputNodes = (flow: undefined | trac.metadata.IFlowDefinition): [string, trac.metadata.IFlowNode][] => (

    Object.entries(flow?.nodes || {}).filter(([, node]) => node.nodeType === trac.FlowNodeType.INPUT_NODE)
)

/**
 * A function that extracts the model nodes from a flow.
 * @param flow - The flow to get the model nodes from.
 * @returns The model nodes.
 */
export const getFlowModelNodes = (flow: undefined | trac.metadata.IFlowDefinition): [string, trac.metadata.IFlowNode][] => (

    Object.entries(flow?.nodes || {}).filter(([, node]) => node.nodeType === trac.FlowNodeType.MODEL_NODE)
)

/**
 * A function that returns an array of the keys for nodes of a particular type from a flow definition. This is useful
 * when iterating over the nodes in a flow for example.
 *
 * @param tag - The flow metadata.
 * @param type - The flow node type keys to return e.g. trac.FlowNodeType.MODEL_NODE.
 * @returns An array of the keys for the given flow not type.
 */
export const getNodeNamesFromFlow = (tag: trac.metadata.ITag, type: trac.FlowNodeType): string[] => {

    return Object.entries(tag?.definition?.flow?.nodes || []).filter(([, node]) => node.nodeType === type).map(([key,]) => key)
}

/**
 * A function that takes the selected models for a flow and checks whether the basicType of the parameters is consistent
 * across the models, if there is a parameter called 'forecast_date' then this can not be defined as a date by one model
 * and a string in another. This is used as part of validating that a flow will run with the user selections.
 *
 * @param selectedModelOptions - The selected model options, the tag property has the model metadata stored in it.
 * @returns An array with information on the parameters with multiple basic types, the types found and the label, this
 * is used in messaging to the user.
 */
export const checkParametersHaveSingleBasicType = (selectedModelOptions: Models["selectedModelOptions"]): ValidationOfModelsParameters => {

    let uniqueParameterKeys: string[] = []
    // An intermediate object that is just used for extracting the information we need
    const allParametersByKey: Record<string, trac.metadata.IModelParameter[]> = {}

    // For each model selected get the parameter definition into a master object
    for (const modelOption of Object.values(selectedModelOptions)) {

        // If the model has the definition, it should already be added in by the function to build the job
        modelOption?.tag.definition?.model?.parameters && Object.entries(modelOption.tag.definition.model.parameters).forEach(([key, value]) => {
            // Add the key to the list of keys
            uniqueParameterKeys.push(key)
            // Add the parameter definition to the array
            if (!allParametersByKey.hasOwnProperty(key)) allParametersByKey[key] = []
            allParametersByKey[key].push(value)
        })
    }

    // Make the keys unique
    uniqueParameterKeys = [...new Set(uniqueParameterKeys)]

    // Multiple models in a flow can use the same parameter, but they can not have different types. Here we look for parameters with multiple types defined.
    // If they do have multiple types the flow can not be run.
    let parametersWithMultipleBasicTypes: FlowValidation["parametersWithMultipleBasicTypes"] = []

    uniqueParameterKeys.forEach(key => {

        // An array of basic types for the parameter, from across all the models
        const basicTypes = [...new Set(allParametersByKey[key].map(parameter => parameter?.paramType?.basicType).filter(isDefined))]

        // If a parameter has multiple basic types we don't allow this
        if (basicTypes.length > 1) {

            // The labels for the parameter from across all the models, keyed by the label and the property being the count
            const labelFrequency = countItemsInArray(allParametersByKey[key].map(parameter => parameter?.label).filter(isDefined), true)
            // The index of the label in labelFrequency with the highest count across all the models
            const mostCommonLabelIndex = Object.values(labelFrequency).findIndex(count => count === Math.max(...Object.values(labelFrequency)))
            // The label to use for the parameter
            const label = mostCommonLabelIndex > -1 ? Object.keys(labelFrequency)[mostCommonLabelIndex] : null

            parametersWithMultipleBasicTypes.push({key, types: basicTypes, label})
        }
    })

    return parametersWithMultipleBasicTypes
}

/**
 * A function that returns whether the model selections have parameters with multiple basic types. This is used as part of
 * validating that a flow will run with the user selections.
 *
 * @param parametersWithMultipleBasicTypes - The validation object from the checkParametersHaveSingleBasicType function.
 */
export const parametersHaveMultipleBasicTypes = (parametersWithMultipleBasicTypes: ValidationOfModelsParameters): boolean => (

    parametersWithMultipleBasicTypes.length > 0
)

/**
 * A function that checks if the model input and output keys defined in the flow match the inputs and outputs defined by the model selected.
 * This is used as part of validating that a flow will run with the user selections.
 * @param flow - The flow definition selected by the user.
 * @param selectedModelOptions - The selected model options.
 * @returns An object with information about the models selected and which inputs and outputs are missing from the flow and vice versa.
 */
export const checkSelectedModelsMatchFlowModelInputsAndOutputs = (flow: trac.metadata.IFlowDefinition, selectedModelOptions: Record<string, SingleValue<SearchOption>>): ValidationOfModelsAndFlowData => {

    let problemsWithInputsAndOutputs: ValidationOfModelsAndFlowData = {}

    // Note that this converts the array of objects to an array of arrays
    getFlowModelNodes(flow).forEach(([modelKey, node]) => {

        // modelKey is the name of the model in the flow, first get the arrays of keys for the inputs and outputs of this model
        const flowModelInputKeys = node?.inputs || []
        const flowModelOutputKeys = node?.outputs || []

        // Now we only want to check when there is a model selected, if there are no model options available or
        // the user selected and the cleared a model (this is not allowed, but you never know) then there are other checks
        // performed to cover these use cases - so we only include this one when there is a model option selected.
        const selectedModelOption = selectedModelOptions[modelKey]

        if (selectedModelOption != null) {

            // Get the input and output data keys from the model
            const selectedModelInputKeys = Object.keys(selectedModelOption?.tag.definition?.model?.inputs || {})
            const selectedModelOutputKeys = Object.keys(selectedModelOption?.tag.definition?.model?.outputs || {})

            // Work out if the flow and the model definitions match
            const missingInputsFromModels = flowModelInputKeys.filter(flowKey => !selectedModelInputKeys.includes(flowKey))
            const missingOutputsFromModels = flowModelOutputKeys.filter(flowKey => !selectedModelOutputKeys.includes(flowKey))

            const missingInputsFromFlow = selectedModelInputKeys.filter(modelKey => !flowModelInputKeys.includes(modelKey))
            const missingOutputsFromFlow = selectedModelOutputKeys.filter(modelKey => !flowModelOutputKeys.includes(modelKey))

            // Save the result
            problemsWithInputsAndOutputs[modelKey] = {
                flow: {missingInputs: missingInputsFromModels, missingOutputs: missingOutputsFromModels},
                models: {missingInputs: missingInputsFromFlow, missingOutputs: missingOutputsFromFlow},
                // Add in the name of the selected model so that we can present more human-readable messages
                modelName: getObjectName(selectedModelOption.tag, false, false, false, false)
            }
        }
    })

    return problemsWithInputsAndOutputs
}

/**
 * A function that returns whether the flow definitions for the model input and outputs match the selected models in the flow.
 * @param modelsMatchFlowModelInputsAndOutputs - The validation object from the checkSelectedModelsMatchFlowModelInputsAndOutputs function.
 */
export const modelsDoNotMatchFlowModelInputsAndOutputs = (modelsMatchFlowModelInputsAndOutputs: ValidationOfModelsAndFlowData): boolean => (

    Object.values(modelsMatchFlowModelInputsAndOutputs).some(inputs => inputs.flow.missingInputs.length > 0 || inputs.flow.missingOutputs.length > 0 || inputs.models.missingInputs.length > 0 || inputs.models.missingOutputs.length > 0)
)

/**
 * A function that checks if the selected input schemas match the schemas defined in the selected models. There is some additional logic needed
 * to map between the flow input node key and the models that use this, this is because edges can be used to rename inputs before they go into
 * the model node socket.
 *
 * There are two types of renames that can happen in a flow, the first is that in a model to model link the output name from model A to the
 * input name to model B is different. We do not have to worry about that here as an input node dataset is not involved in that link or edge.
 * The second type of rename is an input node to a model where the socket name that the input is mapped to is different. We have to deal with
 * these types of renames here to check that the schemas match.
 *
 * @param flow - The flow definition selected by the user.
 * @param selectedModelOptions - The selected model options.
 * @param selectedInputOptions - The selected input options.
 * @returns An object with information about the inputs selected and which inputs have schema mismatches with the models that use them.
 */
export const checkFlowInputSchemasMatchModelInputs = (flow: trac.metadata.IFlowDefinition, selectedModelOptions: Record<string, SingleValue<SearchOption>>, selectedInputOptions: Record<string, SingleValue<SearchOption>>): ValidationOfModelInputs => {

    let problemsWithInputsSchemas: ValidationOfModelInputs = {}

    // Get an array of the model keys in the flow
    const modelKeys = getFlowModelNodes(flow).map(([modelKey]) => modelKey)

    // For each node in the flow that is defined as an input
    getFlowInputNodes(flow).forEach(([inputKey]) => {

        // Look for an edge that passes this into a model, one input can map to many models so there can be multiple edges
        // Note that we are not using the target socket here, so we will pick up any renames as well.
        const edgesDirect = (flow?.edges || []).filter(edge => edge?.source?.node === inputKey && edge.target?.node && modelKeys.includes(edge.target.node))

        // For each edge we found passing the input to a model
        edgesDirect.forEach(edgeDirect => {

            // This is the model key
            const modelKey = edgeDirect.target?.node

            // The key of the input according to the model (not always the same as inputKey as they can be remapped)
            const modelInputKey = edgeDirect.target?.socket

            // The schema of the selected input dataset
            const selectedInputSchema = hasOwnProperty(selectedInputOptions, inputKey) ? selectedInputOptions[inputKey]?.tag?.definition?.data?.schema?.table?.fields : undefined

            if (selectedInputSchema === undefined) {
                console.log(`LOG :: Input dataset key '${inputKey}' does not have a schema to check`)
            }

            // Get the schema for the input defined in the model, if the right keys exist
            if (modelKey && hasOwnProperty(selectedModelOptions, modelKey) && modelInputKey) {

                // Setting a value instead of referring to the index helps Typescript assert the type
                const selectedOption = selectedModelOptions[modelKey]
                const modelInputs = selectedOption?.tag.definition?.model?.inputs

                // The schema as defined by the model
                const schemaInModelDefinition = hasOwnProperty(modelInputs, modelInputKey) ? modelInputs[modelInputKey]?.schema?.table?.fields : undefined

                if (schemaInModelDefinition === undefined) {
                    console.log(`LOG :: Model '${modelKey}' has a model input key '${modelInputKey}' that does not have a schema to check`)
                }

                // If we found both schemas
                if (selectedInputSchema && schemaInModelDefinition && selectedOption !== null) {
                    // Then do a comparison on fieldName and fieldType
                    // The first argument is allowed to have more columns than the second and not cause errors
                    problemsWithInputsSchemas[inputKey] = {...differencesInSchemas(selectedInputSchema, schemaInModelDefinition), modelKey: modelKey, modelName: getObjectName(selectedOption.tag)}
                }
            }
        })
    })

    return problemsWithInputsSchemas
}


/**
 * A function that checks if the selected input schemas match the schemas defined in the selected model.
 *
 * @param modelDefinition - The model definition selected by the user.
 * @param selectedInputOptions - The selected input options.
 * @returns An object with information about the inputs selected and which inputs have schema mismatches with the model selected.
 */
export const checkModelInputSchemasMatchInputs = (modelDefinition: trac.metadata.IModelDefinition, selectedInputOptions: Record<string, SingleValue<SearchOption>>): ValidationOfModelInputs => {

    let problemsWithInputsSchemas: ValidationOfModelInputs = {}

    // For each input defined in the model
    Object.keys(modelDefinition.inputs || {}).forEach(inputKey => {

        // The schema of the selected input dataset
        const selectedInputSchema = hasOwnProperty(selectedInputOptions, inputKey) ? selectedInputOptions[inputKey]?.tag?.definition?.data?.schema?.table?.fields : undefined

        if (selectedInputSchema === undefined) {
            console.log(`LOG :: Input dataset key '${inputKey}' does not have a schema to check`)
        }

        // The schema as defined by the model
        const schemaInModelDefinition = hasOwnProperty(modelDefinition.inputs, inputKey) ? modelDefinition.inputs[inputKey]?.schema?.table?.fields : undefined

        if (schemaInModelDefinition === undefined) {
            console.log(`LOG :: Model input key '${inputKey}' that does not have a schema to check`)
        }

        // If we found both schemas
        if (selectedInputSchema && schemaInModelDefinition) {
            // Then do a comparison on fieldName and fieldType
            // The first argument is allowed to have more columns than the second and not cause errors
            problemsWithInputsSchemas[inputKey] = {...differencesInSchemas(selectedInputSchema, schemaInModelDefinition)}
        }
    })

    return  problemsWithInputsSchemas
}

/**
 * A function that returns whether the selected input datasets for the flow are compatible with the input schema definitions for the selected model.
 * @param inputSchemasNotMatchingModelInputs - The validation object from the checkFlowInputSchemasMatchModelInputs function.
 */
export const inputSchemasDoNotMatchModelInputs = (inputSchemasNotMatchingModelInputs: ValidationOfModelInputs): boolean => (

    Object.values(inputSchemasNotMatchingModelInputs).some(inputs => inputs.missing.length > 0 || inputs.type.length > 0)
)

/**
 * A function that checks whether there are options available for each model in the model chain.
 * @param modelOptions - The model options for the model chain.
 */
export const checkModelOptionsExistForAll = (modelOptions: Record<string, TracGroup>): boolean => (

    !Object.values(modelOptions).some(modelOption => modelOption.every(group => group.options.length === 0))
)

/**
 * A function that checks whether there are options available for each model in the model chain.
 * @param inputOptions - The inputs options for the model chain.
 * @param optionalInputs - The optional inputs for the model chain.
 */
export const checkNonOptionalInputOptionsExistForAll = (inputOptions: Record<string, GenericGroup[]>, optionalInputs: string[]): boolean => (

    !Object.entries(inputOptions).some(([key, inputOption]) => !optionalInputs.includes(key) && inputOption.every(group => group.options.length === 0))
)

/**
 * A function that runs all the validation checks used to check if a job will run with the current user selections. This returns an object
 * the same interface as the validation property in the {@link runAFlowStore}.
 *
 * @param flowDefinition - The flow definition selected by the user.
 * @param selectedModelOptions - The selected model options, the tag property has the model metadata stored in it.
 * @param selectedInputOptions - The selected input options, the tag property has the model metadata stored in it.
 * @param modelOptions - The model options for the model chain.
 * @param inputOptions - The input options for the model chain.
 * @param inputTypes - The input keys for the model broken down into required, optional and by category.
 * @returns An object with summary validation information about the flow and the user set up.
 */
export const validateFlowSelections = (flowDefinition: trac.metadata.IFlowDefinition | undefined | null, selectedModelOptions: Record<string, SingleValue<SearchOption>>, selectedInputOptions: Record<string, SingleValue<SearchOption>>, modelOptions: Record<string, TracGroup>, inputOptions: Record<string, GenericGroup[]>, inputTypes: JobInputsByCategory): Omit<FlowValidation, "isValid" | "validationChecked"> => {

    const validation = {
        parametersWithMultipleBasicTypes: checkParametersHaveSingleBasicType(selectedModelOptions),
        modelsMatchFlowModelInputsAndOutputs: flowDefinition ? checkSelectedModelsMatchFlowModelInputsAndOutputs(flowDefinition, selectedModelOptions) : {},
        flowInputSchemasNotMatchingModelInputs: flowDefinition ? checkFlowInputSchemasMatchModelInputs(flowDefinition, selectedModelOptions, selectedInputOptions) : {},
        modelOptionsExistForAll: checkModelOptionsExistForAll(modelOptions),
        nonOptionalInputOptionsExistForAll: checkNonOptionalInputOptionsExistForAll(inputOptions, inputTypes.optional),
        canRun: false
    }

    // The tests should be that true === bad
    const test1 = parametersHaveMultipleBasicTypes(validation.parametersWithMultipleBasicTypes)
    const test2 = modelsDoNotMatchFlowModelInputsAndOutputs(validation.modelsMatchFlowModelInputsAndOutputs)
    const test3 = inputSchemasDoNotMatchModelInputs(validation.flowInputSchemasNotMatchingModelInputs)
    const test4 = !validation.modelOptionsExistForAll
    const test5 = !validation.nonOptionalInputOptionsExistForAll

    validation.canRun = !test1 && !test2 && !test3 && !test4 && !test5

    return validation
}

/**
 * A function that runs all the validation checks used to check if a job will run with the current user selections. This returns an object
 * the same interface as the validation property in the {@link runAModelStore}.
 *
 * @param modelDefinition - The flow definition selected by the user.
 * @param selectedInputOptions - The selected input options, the tag property has the model metadata stored in it.
 * @param inputOptions - The input options for the model chain.
 * @param inputTypes - The input keys for the model broken down into required, optional and by category.
 * @returns An object with summary validation information about the flow and the user set up.
 */
export const validateModelSelections = (modelDefinition: trac.metadata.IModelDefinition | undefined | null, selectedInputOptions: Record<string, SingleValue<SearchOption>>, inputOptions: Record<string, GenericGroup[]>, inputTypes: JobInputsByCategory): Omit<ModelValidation, "isValid" | "validationChecked"> => {

    const validation = {
        modelInputSchemasNotMatchingModelInputs: modelDefinition ? checkModelInputSchemasMatchInputs(modelDefinition, selectedInputOptions) : {},
        nonOptionalInputOptionsExistForAll: checkNonOptionalInputOptionsExistForAll(inputOptions, inputTypes.optional),
        canRun: false
    }

    // The tests should be that true === bad
    const test3 = inputSchemasDoNotMatchModelInputs(validation.modelInputSchemasNotMatchingModelInputs)
    const test5 = !validation.nonOptionalInputOptionsExistForAll

    validation.canRun = !test3 && !test5

    return validation
}

export const getModelCountFromFlow = (flowDefinition: trac.metadata.IFlowDefinition): number => {

    return Object.values(flowDefinition.nodes || {}).filter(node => node.nodeType === trac.FlowNodeType.MODEL_NODE).length
}

export const getInputCountFromFlow = (flowDefinition: trac.metadata.IFlowDefinition): number => {

    return Object.values(flowDefinition.nodes || {}).filter(node => node.nodeType === trac.FlowNodeType.INPUT_NODE).length
}

export const getInputCountFromModel = (modelDefinition: trac.metadata.IModelDefinition): number => {

    return Object.keys(modelDefinition.inputs || {}).length
}

export const getOutputCountFromFlow = (flowDefinition: trac.metadata.IFlowDefinition): number => {

    return Object.values(flowDefinition.nodes || {}).filter(node => node.nodeType === trac.FlowNodeType.OUTPUT_NODE).length
}

/**
 * A function that creates the structure for the model options in {@link runAFlowStore}.
 * @param flow - The flow definition to set the template for.
 * @returns An object where the properties are the model keys and the values are an empty option
 * group.
 */
export const createFlowModelOptionsTemplate = (flow: trac.metadata.IFlowDefinition): Record<string, TracGroup> => {

    let modelOptions: Record<string, TracGroup> = {}

    // Note that this converts the array of objects to an array of arrays
    Object.entries(flow.nodes || {}).filter(([, node]) => node.nodeType === trac.FlowNodeType.MODEL_NODE).forEach(modelNode =>
        // modelNode[0] is the model key
        modelOptions[modelNode[0]] = createBlankTracGroup()
    )

    return modelOptions
}

/**
 * A function that creates the structure for the selected model options in {@link runAFlowStore}.
 * @param flow - The flow definition to set the template for.
 * @returns An object where the properties are the model keys and the values are null (no option selected).
 */
export const createFlowSelectedModelOptionsTemplate = (flow: trac.metadata.IFlowDefinition): Record<string, SingleValue<SearchOption>> => {

    let selectedModelOptions: Record<string, SingleValue<SearchOption>> = {}

    // Note that this converts the array of objects to an array of arrays
    getFlowModelNodes(flow).forEach(modelNode =>
        // modelNode[0] is the model key
        selectedModelOptions[modelNode[0]] = null
    )

    return selectedModelOptions
}

/**
 * A function that classifies each model in the flow into a type. Sub models (called by a main model) are
 * visualised differently in the UI.
 *
 * @param flowDefinition - The flow definition to use to get the model types from.
 * @returns An object containing a broken down version of the models in a flow.
 */
export const categoriseFlowModelTypes = (flowDefinition: trac.metadata.IFlowDefinition): Models["modelTypes"] => {

    let modelTypes: Models["modelTypes"] = {
        allModels: [],
        mainModels: [],
        subModels: [],
        subModelsByModelKey: {}
    }

    // Note that this converts the array of objects to an array of arrays
    getFlowModelNodes(flowDefinition).forEach(modelNode => {

        const [key] = modelNode

        modelTypes.allModels.push(key)
        modelTypes.subModelsByModelKey[key] = []
        modelTypes.mainModels.push(modelNode[0])
    })

    return modelTypes
}

/**
 * A function that creates the structure for the input options in {@link runAFlowStore}.
 * @param flowDefinition - The flow definition to set the template for.
 * @returns An object where the properties are the input dataset keys and the values are an empty option
 * group.
 */
export const createFlowInputOptionsTemplate = (flowDefinition: trac.metadata.IFlowDefinition): Record<string, TracGroup> => {

    let inputOptions: Record<string, TracGroup> = {}

    // Note that this converts the array of objects to an array of arrays
    getFlowInputNodes(flowDefinition).forEach(inputNode =>

        // modelNode[0] is the model key
        inputOptions[inputNode[0]] = createBlankTracGroup()
    )

    return inputOptions
}

/**
 * A function that creates the structure for the selected input datasets in {@link runAFlowStore}.
 * @param flowDefinition - The flow definition to set the template for.
 * @returns An object where the properties are the input dataset keys and the values are null (no option selected).
 */
export const createFlowSelectedInputOptionsTemplate = (flowDefinition: trac.metadata.IFlowDefinition): Record<string, SingleValue<SearchOption>> => {

    let selectedInputOptions: { [key: string]: SingleValue<SearchOption> } = {}

    // Note that this converts the array of objects to an array of arrays
    getFlowInputNodes(flowDefinition).forEach(inputNode =>

        // inputNode[0] is the input key
        selectedInputOptions[inputNode[0]] = null
    )

    return selectedInputOptions
}

/**
 * A function that creates the structure for the input dataset options in {@link runAModelStore}.
 * @param model - The model definition to set the template for.
 * @returns An object where the properties are the input dataset keys and the values are an empty option
 * group.
 */
export const createModelInputOptionsTemplate = (model: trac.metadata.IModelDefinition): Record<string, TracGroup> => {

    let inputOptions: { [key: string]: TracGroup } = {}

    // Note that this converts the array of objects to an array of arrays, also note that we key it by the key the model
    // expects not the key in the attrs of the dataset
    Object.entries(model.inputs || {}).forEach(inputNode =>

        inputOptions[inputNode[0]] = createBlankTracGroup()
    )

    return inputOptions
}

/**
 * A function that creates the structure for the selected input datasets in {@link runAModelStore}.
 * @param model - The model definition to set the template for.
 * @returns An object where the properties are the input dataset keys and the values are null (no option selected).
 */
export const createModelSelectedInputOptionsTemplate = (model: trac.metadata.IModelDefinition): { [key: string]: SingleValue<SearchOption> } => {

    let selectedInputOptions: { [key: string]: SingleValue<SearchOption> } = {}

    // Note that this converts the array of objects to an array of arrays
    Object.entries(model.inputs || {}).forEach(inputNode =>

        selectedInputOptions[inputNode[0]] = null
    )

    return selectedInputOptions
}

/**
 * A function that takes a model definition from its metadata and creates an array of the parameter definitions it uses.
 * @param model - The model to get the parameters from.
 * @returns The array of model parameter definitions.
 */

/**
 * A function that takes a selected model and extracts all the parameters required from them.
 *
 * @param modelMetadata - The selected model metadata.
 */
export const buildParametersForSingleModel = (modelMetadata: Model["modelMetadata"]): { key: string, value: trac.metadata.IModelParameter }[] => {

    const parameters: { key: string, value: trac.metadata.IModelParameter }[] = []

    // If the model has the definition (it should already be added in by the function to build the job)
    Object.entries(modelMetadata?.definition?.model?.parameters || {}).forEach(([key, value]) => {

        parameters.push({key: key, value: value})
    })

    return parameters
}

/**
 * A function that takes the flow attributes and extracts a list of the keys of support inputs.
 * This is not a core part of the TRAC API so support units are defined as an attribute.
 *
 * @param attrs - The flow definition attributes.
 * @returns An array of input dataset keys set as being for support datasets. These are placed in their own heading in
 * the {@link RunAFlowScene} as they are not part of the core inputs.
 */
export const getSupportInputsFromAttributes = (attrs: trac.metadata.ITag["attrs"]): string[] => {

    const supportInputs = attrs ? extractValueFromTracValueObject(attrs.support_inputs) : null

    if (supportInputs === null) return []

    const {basicType, subBasicType, value} = supportInputs

    if (basicType === trac.BasicType.ARRAY && subBasicType === trac.STRING && isStringArray(value)) {
        return value
    } else {
        return []
    }
}

/**
 * A function that takes the metadata for a flow and extracts information about what category each input should
 * be shown in. The {@link ParameterMenu} component has the ability to break down the datasets into different
 * categories.
 *
 * @param flowDefinition - The flow metadata to get the input information from.
 * @returns An object containing the keys of input datasets broken down into whether they are optional or not
 * and their designated category.
 */
export const categoriseFlowInputTypes = (flowDefinition: trac.metadata.IFlowDefinition): JobInputsByCategory => {

    let inputCategories: JobInputsByCategory = {required: [], optional: [], categories: {}}

    Object.entries(flowDefinition.nodes || {}).filter(([, node]) => node.nodeType === trac.FlowNodeType.INPUT_NODE).forEach(([key, input]) => {

        // TODO remove economic_scenario
        if (key !== "economic_scenario" && (!hasOwnProperty(input, "required") || (hasOwnProperty(input, "required") && input.required === true))) {

            inputCategories.required.push(key)

        } else {

            inputCategories.optional.push(key)
        }
    })

    return inputCategories
}

/**
 * A function that takes the metadata for a model and extracts information about what category each input should
 * be shown in. The {@link ParameterMenu} component has the ability to break down the datasets into different
 * categories.
 *
 * @param modelDefinition - The model metadata to get the input information from.
 * @returns An object containing the keys of input datasets broken down into whether they are optional or not
 * and their designated category.
 */
export const categoriseModelInputTypes = (modelDefinition: trac.metadata.IModelDefinition): JobInputsByCategory => {

    let inputCategories: JobInputsByCategory = {required: [], optional: [], categories: {}}

    Object.entries(modelDefinition.inputs || {}).forEach(([key, input]) => {

        // TODO remove economic_scenario
        if (key !== "economic_scenario" && (!hasOwnProperty(input, "required") || (hasOwnProperty(input, "required") && input.required === true))) {

            inputCategories.required.push(key)

        } else {

            inputCategories.optional.push(key)
        }
    })

    return inputCategories
}
/**
 * A function that runs when a flow or model is loaded and being set up as a job. This converts the parameter definitions extracted from
 * the models to a format that can be passed to the {@link ParameterMenu} component. This structure matches the attributes structure as
 * they use the same component.
 *
 * It then uses the parameter list dataset which contains the flow specific configurations for these parameters.
 * This list file enriches the UI with titles, descriptions as well as parameter specific options and defaults.
 *
 * @param modelParameters - An array of the parameters defined across the models a flow or a model on its own,
 * the parameter key and the definition are available.
 * @param additionalParameterDetails - An object containing additional details for model parameters set by the user.
 * This dataset is owned by the {@link applicationSetupStore}.
 * @returns An object containing details for each parameter.
 */
export const setProcessedParameters = (modelParameters: { key: string, value: trac.metadata.IModelParameter }[], additionalParameterDetails?: Record<string, UiAttributesProps>): Record<string, UiAttributesProps> => {

    // This is the object that we are going to return
    let parameters: Record<string, UiAttributesProps> = {}

    modelParameters.forEach(modelParameter => {

        // What are the basic types of the version of the parameter in the model and the type in the additionalParameterDetails
        // argument, if these are different we need to have some extra logic to make sure we don't create a definition where
        // it doesn't work.
        const basicTypeFromModel = modelParameter.value.paramType?.basicType || trac.BasicType.BASIC_TYPE_NOT_SET
        const basicTypeFromAdditionalDetails = additionalParameterDetails && additionalParameterDetails[modelParameter.key]?.basicType || trac.BasicType.BASIC_TYPE_NOT_SET

        // Whether we can merge the different definitions together depends on whether the types are the same
        const differentTypeFound = basicTypeFromModel !== basicTypeFromAdditionalDetails

        const {
            defaultValue: additionalDefaultValue,
            description = null,
            formatCode: additionalFormatCode = null,
            hidden = false,
            isMulti = Boolean(basicTypeFromModel === trac.BasicType.ARRAY),
            linkedToId = null,
            linkedToValue = null,
            maximumValue: additionalMaximumValue = null,
            minimumValue: additionalMinimumValue = null,
            name: additionalName,
            options: additionalOptions = [],
            tooltip
        } = additionalParameterDetails && additionalParameterDetails[modelParameter.key] || {}

        // DEFAULT VALUE

        // Does the parameter definition from the model has a default value set
        let defaultValue = modelParameter.value.defaultValue ? extractValueFromTracValueObject(modelParameter.value.defaultValue).value : null

        // See if the additionalDefaultValue can be applied
        if (!differentTypeFound) {
            // Now the model definition cannot be typed as options but the additional details can, so we have to keep the type interfaces intact
            // if the additional defaultValues are options then we will add them in later
            defaultValue = !Array.isArray(additionalDefaultValue) && !isOption(additionalDefaultValue) && additionalDefaultValue || null
        }

        // FORMAT CODE

        let formatCode = null

        if (!differentTypeFound) {
            // If there is no difference in types then we can use the format in the additional details can be used
            formatCode = additionalFormatCode
        }

        // DEFAULT VALUE OPTIONS

        // If the model has an array as the default value convert that to a set of options for the select component
        let defaultValueOptions: Option[] = Array.isArray(defaultValue) ? convertArrayToOptions(defaultValue, true, basicTypeFromModel, formatCode) : []

        // We do not merge the default options from across the model definition and the additional details but if the former
        // is blank we do use the latter
        if (!differentTypeFound && defaultValueOptions.length === 0) {
            // If there is no difference in types then we can use the format in the additional details can be used
            if (Array.isArray(additionalDefaultValue)) {
                defaultValueOptions = additionalDefaultValue
            } else if (isOption(additionalDefaultValue)) {
                defaultValueOptions = [additionalDefaultValue]
            }
        }

        // NAME

        // Does the parameter definition from the model has a label extract that
        const {label} = modelParameter.value

        const name = (label != null && label.length > 0) ? label : additionalName != null && additionalName.length > 0 ? additionalName : convertKeyToText(modelParameter.key)

        // OPTIONS

        let options = defaultValueOptions

        // The additional details interface has options possible being an option group but in reality this
        // is only for business segments, so we can exclude doing anything with option groups to keep
        // Typescript happy
        if (!differentTypeFound && !isGroupOption(additionalOptions)) {
            // If there is no difference in types then we can add in the additional detail options too,
            // but we need to be able to reduce them to unique values
            options = makeArrayOfObjectsUniqueByProperty([...additionalOptions, ...defaultValueOptions], "value")
        }

        parameters[modelParameter.key] = {

            // objectTypes is not the object enum (e.g. 1, 2) but the string version (e.g. DATA, FLOW)
            objectTypes: ["JOB"],
            id: modelParameter.key,
            basicType: basicTypeFromModel,
            name,
            description,
            category: "GENERAL",
            minimumValue: !differentTypeFound ? additionalMinimumValue : null,
            maximumValue: !differentTypeFound ? additionalMaximumValue : null,
            tooltip,
            hidden,
            linkedToId,
            linkedToValue,
            isMulti,
            defaultValue: !Array.isArray(defaultValue) && !isObject(defaultValue) ? defaultValue : defaultValueOptions,
            formatCode: !differentTypeFound ? formatCode : null,
            // We boolean default validation is whether the value is true, we can't validate these as
            // false is a valid parameter value
            // TODO make booleans not valid on null value?
            mustValidate: basicTypeFromModel !== trac.BOOLEAN,
            useOptions: Array.isArray(defaultValue),
            disabled: false,
            widthMultiplier: 1,
            // Add the default values from the model to the designated values in the dataset
            options,
            numberOfRows: 1,
            specialType: Array.isArray(defaultValue) ? "OPTION" : undefined
        }
    })

    return parameters
}

/**
 * A function that runs when a flow or model is loaded and being set up as a job. This converts the input dataset definitions extracted from
 * the models to a format that can be passed to the {@link ParameterMenu} component. This structure matches the attributes structure as
 * they use the same component.
 *
 * It uses the flow definition and the categorisation of inputs (optional, required, support etc.) to set properties such as the label
 * and category for the inputs.
 *
 * @param modelInputs - The selected inputs for either the models in a flow or a model running on its own. This is keyed by the input key.
 * @param inputTypes - The definition of which input datasets are support or non-support or optional or required. This is currently
 * just calculated for a flow, for a model everything is assigned as required.
 * @param flowDefinition - The flow metadata, this is optional because this function is used for both the inputs to flows and the inputs to a model
 * running on its own.
 */
export const setProcessedInputs = (modelInputs: Record<string, SingleValue<Option<string, trac.metadata.ITag>>>, inputTypes: JobInputsByCategory, flowDefinition?: trac.metadata.IFlowDefinition, flowAttrs ?: trac.metadata.ITag["attrs"]): Record<string, UiAttributesProps> => {

    // This is the object that we are going to return
    let inputs: Record<string, UiAttributesProps> = {}

    // The labels for the inputs
    const labels: [string, (undefined | null | string)][] = flowDefinition ? (getFlowInputNodes(flowDefinition).map(([key, node]) => ([key, node?.label]))) : []

    // TODO remove this when flow definition is updated to have categories
    const pd_inputs_extracted = flowAttrs == undefined ? [] : extractValueFromTracValueObject(flowAttrs["pd_inputs"]).value
    const ead_inputs_extracted = flowAttrs == undefined ? [] : extractValueFromTracValueObject(flowAttrs["ead_inputs"]).value
    const lgd_inputs_extracted = flowAttrs == undefined ? [] : extractValueFromTracValueObject(flowAttrs["lgd_inputs"]).value
    const lic_inputs_extracted = flowAttrs == undefined ? [] : extractValueFromTracValueObject(flowAttrs["lic_inputs"]).value
    const rwa_inputs_extracted = flowAttrs == undefined ? [] : extractValueFromTracValueObject(flowAttrs["rwa_inputs"]).value
    const parameter_inputs_extracted = flowAttrs == undefined ? [] : extractValueFromTracValueObject(flowAttrs["parameter_inputs"]).value
    const t0_inputs_extracted = flowAttrs == undefined ? [] : extractValueFromTracValueObject(flowAttrs["t0_inputs"]).value
    const lookup_inputs_extracted = flowAttrs == undefined ? [] : extractValueFromTracValueObject(flowAttrs["lookup_inputs"]).value
    const overlay_inputs_extracted = flowAttrs == undefined ? [] : extractValueFromTracValueObject(flowAttrs["overlay_inputs"]).value
    const linked_inputs_extracted = flowAttrs == undefined ? [] : extractValueFromTracValueObject(flowAttrs["linked_inputs"]).value

    const pd_inputs = Array.isArray(pd_inputs_extracted) ? pd_inputs_extracted.filter(item => typeof item === "string") : []
    const ead_inputs = Array.isArray(ead_inputs_extracted) ? ead_inputs_extracted.filter(item => typeof item === "string") : []
    const lgd_inputs = Array.isArray(lgd_inputs_extracted) ? lgd_inputs_extracted.filter(item => typeof item === "string") : []
    const lic_inputs = Array.isArray(lic_inputs_extracted) ? lic_inputs_extracted.filter(item => typeof item === "string") : []
    const rwa_inputs = Array.isArray(rwa_inputs_extracted) ? rwa_inputs_extracted.filter(item => typeof item === "string") : []
    const parameter_inputs = Array.isArray(parameter_inputs_extracted) ? parameter_inputs_extracted.filter(item => typeof item === "string") : []
    const t0_inputs = Array.isArray(t0_inputs_extracted) ? t0_inputs_extracted.filter(item => typeof item === "string") : []
    const lookup_inputs = Array.isArray(lookup_inputs_extracted) ? lookup_inputs_extracted.filter(item => typeof item === "string") : []
    const overlay_inputs = Array.isArray(overlay_inputs_extracted) ? overlay_inputs_extracted.filter(item => typeof item === "string") : []
    const linked_inputs = Array.isArray(linked_inputs_extracted) ? linked_inputs_extracted.filter(item => typeof item === "string") : []

    const getCategory = (key: string): string | undefined => {

        if (pd_inputs.includes(key)) {
            return "PD inputs"
        } else if (ead_inputs.includes(key)) {
            return "EAD inputs"
        } else if (lgd_inputs.includes(key)) {
            return "LGD inputs"
        } else if (lic_inputs.includes(key)) {
            return "LIC inputs"
        } else if (rwa_inputs.includes(key)) {
            return "RWA inputs"
        } else if (parameter_inputs.includes(key)) {
            return "Parameter inputs"
        } else if (t0_inputs.includes(key)) {
            return "T0 inputs"
        } else if (lookup_inputs.includes(key)) {
            return "Lookup and reference inputs"
        } else if (overlay_inputs.includes(key)) {
            return "Inputs to use as overlays"
        } else if (linked_inputs.includes(key)) {
            return "Inputs from other jobs"
        } else {
            return undefined
        }
    }

    Object.entries(modelInputs).forEach(([datasetKey, selectedInputOption]) => {

        // The label for the input, this comes from the flow definition
        const label = labels.find(([key]) => key === datasetKey)
        // The description for the input, this comes from the selected dataset option
        const description = extractValueFromTracValueObject(selectedInputOption?.tag?.attrs?.description).value
        // The label for the input, this comes from the flow definition and the flow attrs
        const category = inputTypes.optional.includes(datasetKey) ? "Optional" : "General"

        // Need to add full info here for attr/param
        inputs[datasetKey] = {

            // objectTypes is not the object enum (e.g. 1, 2) but the string version (e.g. DATA, FLOW)
            objectTypes: ["JOB"],
            id: datasetKey,
            // The options for inputs will always be string values
            basicType: trac.STRING,
            name: label === undefined || typeof label[1] !== "string" ? convertKeyToText(datasetKey) : label[1],
            description: typeof description === "string" && description.length > 0 ? description : null,
            category: getCategory(datasetKey) || category,
            minimumValue: null,
            maximumValue: null,
            tooltip: undefined,
            hidden: false,
            linkedToId: null,
            linkedToValue: null,
            isMulti: false,
            defaultValue: null,
            formatCode: null,
            // Optional inputs do not need to validate
            mustValidate: !Boolean(inputTypes.optional.includes(datasetKey)),
            useOptions: true,
            disabled: false,
            widthMultiplier: 1,
            numberOfRows: 1,
            options: undefined,
            specialType: "OPTION"
        }
    })

    return inputs
}

/**
 * A function that parses a flow and extracts information about any dataset that are renamed. These
 * are outputs from model A being used in model B but under a different name. These are identified
 * by any links that are model to model as this is needed to remap a dataset name between the two.
 *
 * @example
 * An edge where a dataset is renamed between models.
 * ```ts
 * {"start": {
 *      "type": "model",
 *      "name": "default_rate_calculation",
 *      "socket": "segment_default_rate_monthly_forecast"
 *  },
 *  "end": {
 *      "type": "model",
 *      "name": "default_rate_calculation_conversion",
 *      "socket": "segment_default_rate_monthly_forecast2"
 * }}
 *
 * @param flow - The flow definition metadata.
 * @returns An array of renamed dataset information.
 */
export const getRenamedDatasets = (flow?: null | trac.metadata.IFlowDefinition): RenamedDataset[] => {

    let renamedDatasets: RenamedDataset[] = []

    flow?.edges && flow.edges.forEach(edge => {

        // Are all the pieces of information we need set
        if (edge.source?.node && edge.target?.node && edge.source?.socket && edge.target?.socket) {

            // Is the node a model to model node
            if (hasOwnProperty(flow.nodes, edge.source.node) && flow.nodes[edge.source.node].nodeType === trac.FlowNodeType.MODEL_NODE && flow.nodes[edge.target.node].nodeType === trac.FlowNodeType.MODEL_NODE) {

                // Is the dataset name different
                if (edge.source?.socket !== edge.target?.socket) {

                    renamedDatasets.push({
                        modelStart: edge.source?.node,
                        modelEnd: edge.target?.node,
                        datasetStart: edge.source?.socket,
                        datasetEnd: edge.target?.socket
                    })

                }
            }
        }
    })

    return renamedDatasets
}

/**
 * A function that takes the renamed datasets from a flow and converts these to extra edges in a flow
 * required to drawn them into a flow chart.
 *
 * @param renamedDatasets - An array of the renamed datasets, this is calculated from the
 * {@link getRenamedDatasets} function.
 * @returns An array of additional edges to add to a visualisation of the flow so that renames are
 * more transparent.
 */
export const getAdditionalEdgesForRenamedDatasets = (renamedDatasets: RenamedDataset[]): GregsEdge[] => {

    const additionalEdgesForRenamedDatasets: GregsEdge[] = []

    // We have two edges to add for each renamed dataset. The first is from the original dataset from model A
    // to the renamed dataset and then from the renamed dataset to model B. With these the original dataset from
    // model A would be directly linked to model B.

    // We treat all renamed datasets as outputs - they are always because they are created from the outputs of other models.
    renamedDatasets.forEach(remap => {

        additionalEdgesForRenamedDatasets.push({
            id: `${remap.datasetEnd}-${remap.modelEnd}-${remap.datasetEnd}-${remap.modelEnd}}`,
            sources: [`${trac.FlowNodeType.OUTPUT_NODE}-${remap.datasetEnd}`],
            targets: [`${trac.FlowNodeType.MODEL_NODE}-${remap.modelEnd}`],
            start: {type: trac.FlowNodeType.OUTPUT_NODE, node: remap.datasetEnd},
            end: {type: trac.FlowNodeType.MODEL_NODE, node: remap.modelEnd},
            isRename: false
        })

        additionalEdgesForRenamedDatasets.push({
            id: `${remap.datasetStart}-${remap.datasetEnd}-${remap.datasetStart}-${remap.datasetEnd}}`,
            sources: [`${trac.FlowNodeType.OUTPUT_NODE}-${remap.datasetStart}`],
            targets: [`${trac.FlowNodeType.OUTPUT_NODE}-${remap.datasetEnd}`],
            start: {type: trac.FlowNodeType.OUTPUT_NODE, node: remap.datasetStart},
            end: {type: trac.FlowNodeType.OUTPUT_NODE, node: remap.datasetEnd},
            // Only the dataset to dataset link is flagged as the renamed node
            isRename: true
        })
    })

    return additionalEdgesForRenamedDatasets
}

/**
 * A function that takes flow definition metadata and extracts all the nodes from it and puts these
 * into a set of options for the SelectOption component. This allows for example the user to select a node
 * in the flow to get more information about it. The showRenames parameter allows dataset renames that
 * occur in the flow as separate options.
 *
 * @param flow - The flow definition metadata.
 * @param showRenames - Whether to include the renamed datasets as separate options.
 * @returns An option group for selecting a node in a flow.
 */
export const convertFlowNodesToOptions = (flow: trac.metadata.IFlowDefinition, showRenames: boolean): FlowGroup => {

    const {edges, nodes} = flow

    const finalOptions: FlowGroup = [
        {label: "Models", options: []},
        {label: "Inputs", options: []},
        {label: "Outputs", options: []},
        {label: "Intermediates", options: []}
    ]

    if (!flow || !nodes || !edges) return finalOptions

    // Get the details of any renames occurring in the flow
    const renamedDatasets = showRenames ? getRenamedDatasets(flow) : []

    // Convert the renamed datasets to additional edges in the flow, mirroring the existing edge definitions
    const additionalEdgesForRenamedDatasets = showRenames ? getAdditionalEdgesForRenamedDatasets(renamedDatasets) : []

    // This is the array options that we are going to collate
    let options: Option<string, NodeDetails>[] = []

    // Convert each edge to an option
    additionalEdgesForRenamedDatasets.forEach(edge => {

        // The type is the corresponding type in node definition
        const sourceNodeType = edge.start.type
        const targetNodeType = edge.end.type

        // For each node we append each name with its type - models could have the same name as a dataset
        // example, although I think TRAC does not allow this.
        if (sourceNodeType && targetNodeType) {

            options.push({
                value: `${sourceNodeType}-${edge.start.node}`,
                label: edge.start.node,
                details: {type: sourceNodeType, isRename: Boolean(hasOwnProperty(edge, "isRename") && edge?.isRename)}
            })

            options.push({
                value: `${targetNodeType}-${edge.end.node}`,
                label: edge.end.node,
                details: {type: targetNodeType, isRename: Boolean(hasOwnProperty(edge, "isRename") && edge?.isRename)}
            })
        }
    })

    // Convert each edge to an option
    edges.forEach(edge => {

        // The node is the name of the dataset or model node
        const sourceNode = edge.source?.node
        const targetNode = edge.target?.node

        // The type is the corresponding type in node definition
        const sourceNodeType = sourceNode ? nodes[sourceNode].nodeType : undefined
        const targetNodeType = targetNode ? nodes[targetNode].nodeType : undefined

        // For each node we append each name with its type - models could have the same name as a dataset
        // example, although I think TRAC does not allow this.
        if (sourceNode && targetNode && sourceNodeType && targetNodeType) {

            options.push({
                value: `${sourceNodeType}-${sourceNode}`,
                label: sourceNode,
                details: {
                    type: sourceNodeType,
                    isRename: false
                }
            })

            options.push({
                value: `${targetNodeType}-${targetNode}`,
                label: targetNode,
                details: {
                    type: targetNodeType,
                    isRename: false
                }
            })

            // Add an entry for intermediates that do not have their own edge outputting them
            if (edge.source?.socket && edge.target?.socket && edge.source.socket === edge.target.socket) {
                options.push({
                    value: `${nodes[edge.target.socket] ? nodes[edge.target.socket].nodeType || trac.FlowNodeType.NODE_TYPE_NOT_SET : trac.FlowNodeType.NODE_TYPE_NOT_SET}-${edge.target.socket}`,
                    label: edge.target.socket,
                    details: {
                        type: nodes[edge.target.socket] ? nodes[edge.target.socket].nodeType || trac.FlowNodeType.NODE_TYPE_NOT_SET : trac.FlowNodeType.NODE_TYPE_NOT_SET,
                        isRename: false
                    }
                })
            }
        }
    })

    // Make the list unique
    options = makeArrayOfObjectsUniqueByProperty(options, "value")

    // Put the options into groups using the structure needed by the SelectOption component
    finalOptions[0].options = options.filter(option => option.details.type === trac.FlowNodeType.MODEL_NODE)
    finalOptions[1].options = options.filter(option => option.details.type === trac.FlowNodeType.INPUT_NODE)
    finalOptions[2].options = options.filter(option => option.details.type === trac.FlowNodeType.OUTPUT_NODE)
    finalOptions[3].options = options.filter(option => option.details.type === trac.FlowNodeType.NODE_TYPE_NOT_SET)

    return finalOptions
}

/**
 * A function that takes a node ID from a flow (a concatenation of the type and the name of the item)
 * and searches for the equivalent option in the array created by convertFlowNodesToOptions. This is
 * needed because sometimes you only get the node ID and you need to know what value to set a
 * SelectOption component to, this happens for example when the user clicks on an SVG image.
 *
 * @param id - The node ID to look for the option for.
 * @param groups - An option group for a {@link SelectOption} component.
 * @returns The option corresponding to the provided id.
 */
export const getOptionFromNodeId = (id: string, groups: FlowGroup): undefined | Option<string, NodeDetails> => {

    // Search for the ID in the set of options, spoof the ID string into an option
    const {tier, index} = getGroupOptionIndex({value: id, label: id}, groups)

    // Return the option
    return tier > -1 && index > -1 ? groups[tier].options[index] : undefined
}

/**
 * A function that gets information about the inputs used by a model from a flow. In the flow
 * the inputs used by all the models in it are listed together, so first we need to get what
 * inputs a model uses and then get the information for only those inputs.
 *
 * @param flow - The flow definition metadata.
 * @param modelKey - The model ID in the flow to get the inputs for. This is not
 * prepended with the type.
 * @param renamedDatasets - An array of the renamed datasets, this is calculated from the
 * getRenamedDatasets function.
 * @returns Information about the inputs from a flow definition that are used by the provided model key.
 */
export const getModelInputsFromFlow = (flow: trac.metadata.IFlowDefinition, modelKey: string, renamedDatasets: RenamedDataset[] = []): Record<string, trac.metadata.IFlowNode> => {

    if (!flow.nodes) return {}

    // The list of inputs used by the model
    // If there is a renamed dataset then this will be in this list
    const inputsUsed = flow.nodes[modelKey]?.inputs

    let inputs: Record<string, trac.metadata.IFlowNode> = {}

    // Add the information about each input (e.g. the schema) to the inputs object
    // each is keyed by the parameter name
    inputsUsed && inputsUsed.forEach(key => {

        // See if the dataset requested is one of the renamed datasets
        const renamedDataset = renamedDatasets.find(r => r.modelEnd === modelKey && r.datasetEnd === key)

        // If the input's parameter has the input key then it's not a renamed dataset
        if (renamedDataset) {

            inputs[renamedDataset.datasetStart] = {}

        } else {

            inputs[key] = flow.nodes?.[key] || {}
        }
    })

    return inputs
}

/**
 * A function that gets information about the outputs used by a model from a flow. In the flow
 * the outputs used by all the models in it are listed together, so first we need to get what
 * outputs a model uses and then get the information for only those outputs.
 *
 * @param flow -The flow definition metadata.
 * @param modelKey - The model ID in the flow to get the outputs for. This is not
 * prepended with the type.
 * @returns Information about the outputs from a flow definition that are created by the provided model key.
 */
export const getModelOutputsFromFlow = (flow: trac.metadata.IFlowDefinition, modelKey: string): Record<string, trac.metadata.IFlowNode & { type: "output" | "intermediate" }> => {

    if (!flow.nodes) return {}

    // The list of outputs used by the model
    const outputsUsed = flow.nodes[modelKey]?.outputs

    let outputs: Record<string, trac.metadata.IFlowNode & { type: "output" | "intermediate" }> = {}

    outputsUsed && outputsUsed.forEach(key => {

        if (flow.nodes) {
            if (Object.keys(flow.nodes).includes(key)) {
                outputs[key] = {...flow.nodes[key], type: "output"}
            } else {
                outputs[key] = {type: "intermediate"}
            }
        }
    })

    return outputs
}

/**
 * A function that gets information about the outputs used by a model from a flow. In the flow
 * the outputs used by all the models in it are listed together, so first we need to get what
 * outputs a model uses and then get the information for only those outputs.
 *
 * @param flow - The flow definition metadata.
 * @param key - The model ID in the flow to get the outputs for. This is not
 * prepended with the type.
 * @returns Information about the search queries used to get inputs for the provided model key.
 */
export const getSearchQueryFromFlowNode = (flow: trac.metadata.IFlowDefinition, key: string): Record<string, undefined | { attrName: string, value: string }> => {

    if (!flow.nodes) return {}

    let queries: Record<string, undefined | { attrName: string, value: string }> = {}

    // Add the information about each output (e.g. the schema) to the outputs object
    // each is keyed by the output name
    Object.values(flow.nodes[key]).forEach(flowNode => {

        if (flowNode && flowNode.nodeSearch) {
            // TODO this needs to correctly process the query
            queries[key] = {
                attrName: flowNode.nodeSearch.term.attrName || "Unknown",
                value: JSON.stringify(flowNode.nodeSearch.term.searchValue)
            }
        } else {
            queries[key] = undefined
        }
    })

    return queries
}

/**
 * A function that gets the information about a model from a flow. The main reason to doing this
 * way is that they returned variable needs to be an object keyed by the name of the model as that
 * is how the components to be built to match the flow metadata structure.
 *
 * @param flow - The flow definition metadata.
 * @param type - The type of dataset e.g. input or output.
 * @param datasetId - The dataset ID in the flow to get the information for. This is not
 * prepended with the type.
 * @param renamedDatasets - An array of the renamed datasets, this is calculated from the
 * getRenamedDatasets function.
 */
export const getDatasetFromFlow = (flow: trac.metadata.IFlowDefinition, type: trac.FlowNodeType.INPUT_NODE | trac.FlowNodeType.OUTPUT_NODE, datasetId: string, renamedDatasets: RenamedDataset[]): Record<string, trac.metadata.IFlowNode> => {

    if (!flow.nodes) return {}

    // See if the dataset requested is one of the renamed datasets
    const renamedDataset = renamedDatasets.find(r => r.datasetEnd === datasetId)

    let datasets: Record<string, trac.metadata.IFlowNode> = {}

    // Get the dataset information, this is for when it's not a renamed dataset
    if (!renamedDataset) {

        datasets[datasetId] = flow.nodes[datasetId]

    } else {

        // Renamed datasets are always outputs in how they are created in the getRenamedDatasets
        // function. They are always the outputs from model A being used in model B but under a
        // different name.
        datasets[renamedDataset.datasetStart] = flow.nodes[renamedDataset.datasetStart]
    }

    return datasets
}

export const getOptionalInputsFromCategories = (inputDefinitions: Record<string, UiAttributesProps>): string[] =>
    Object.entries(inputDefinitions).filter(([_, item]) =>
        item.category.toUpperCase() === "OPTIONAL"
    ).map(([key]) => key)

export const getRequiredInputsFromCategories = (inputDefinitions: Record<string, UiAttributesProps>): string[] =>
    Object.entries(inputDefinitions).filter(([_, item]) =>
        item.category.toUpperCase() !== "OPTIONAL"
    ).map(([key]) => key)


/**
 * A function that takes the selected models for a flow and extracts all the parameters required from them.
 * It handles cases where there are different default values set across the models and different labels. This
 * function can handle two types of argument depending on what the use case is.
 *
 * @param modelOptions - The selected model options, the tag property has the model metadata stored in it.
 * Or an object of model keys and their associated metadata tag.
 */
export const buildParametersAcrossMultipleModels = (modelOptions: Record<string, SingleValue<SearchOption> | trac.metadata.ITag>): { key: string, value: trac.metadata.IModelParameter }[] => {

    let uniqueParameterKeys: string[] = []

    // An intermediate object that is just used for extracting the information we need
    const allParametersByKey: Record<string, trac.metadata.IModelParameter[]> = {}

    // For each model selected get the parameter definition into a master object
    Object.values(modelOptions).forEach(modelOption => {

        // Get the tag depending on the type of object provided
        const tag = isTagOption(modelOption) ? modelOption.tag : modelOption

        // If the model has the definition (it should already be added in by the function to build the job)
        tag?.definition?.model?.parameters && Object.entries(tag.definition.model.parameters).forEach(([key, value]) => {

            // Add the key to the list of keys
            uniqueParameterKeys.push(key)
            // Add the parameter definition to the array
            if (!allParametersByKey.hasOwnProperty(key)) allParametersByKey[key] = []
            allParametersByKey[key].push(value)
        })
    })

    // Make the keys unique
    uniqueParameterKeys = [...new Set(uniqueParameterKeys)]

    // The final array of parameters that we are going to use in the user interface
    const parameters: { key: string, value: trac.metadata.IModelParameter }[] = []

    uniqueParameterKeys.forEach(key => {

        // The labels for the parameter from across all the models, keyed by the label and the property being the count
        const labelFrequency = countItemsInArray(allParametersByKey[key].map(parameter => parameter?.label).filter(isDefined), true)
        // The index of the label in labelFrequency with the highest count across all the models
        const mostCommonLabelIndex = Object.values(labelFrequency).findIndex(count => count === Math.max(...Object.values(labelFrequency)))
        // The label to use for the parameter
        const label = mostCommonLabelIndex > -1 ? Object.keys(labelFrequency)[mostCommonLabelIndex] : null

        // An array of the default values set for the parameter from across all over the models
        let defaultValues = allParametersByKey[key].map(parameter => parameter?.defaultValue ? extractValueFromTracValueObject(parameter?.defaultValue).value : null)
        // The index of a defaultValue that is not null
        const nonNullDefaultValueIndex = defaultValues.findIndex(defaultValue => defaultValue != null)

        // Add the parameter to show in the UI to the list and merge in the label to use, we pick a parameter with a non-null default value if one is set
        parameters.push({key: key, value: {...allParametersByKey[key][Math.max(nonNullDefaultValueIndex, 0)], ...{label}}})
    })

    return parameters
}