/**
 * A component that shows a rich text editor (RTE) that allows the user to create a block of formatted text. For example
 * a bulleted list can be added or bold and italic text. This is based off of the slate plugin
 * https://docs.slatejs.org/v/v0.47/. Note that the switching of edit mode needs to be controlled via a prop.
 *
 * @module RichTextEditor
 * @category Component
 */

import Button from "react-bootstrap/Button";
import type {BaseEditor, Descendant} from "slate";
import {createEditor, Editor, Element as SlateElement, Transforms} from "slate";
import {Editable, Slate, useSlate, withReact} from "slate-react";
import type {ReactEditor} from "slate-react";
import {HistoryEditor, withHistory} from "slate-history";
import {Icon} from "./Icon";
import PropTypes from "prop-types";
import React, {memo, useMemo, useState} from "react";
import {useAppDispatch} from "../../types/types_hooks";

// The type for the editor, we use the base version and extend it for react and to be able to access the history
type CustomEditor = ReactEditor & BaseEditor & HistoryEditor

// Our text elements can also have formats
type CustomText = { text: string, bold?: boolean, italic?: boolean, underline?: boolean, code?: boolean }

// We allow both mark and block text formats
type MarkFormat = "bold" | "italic" | "underline" | "code"
type BlockFormat = "block-quote" | "bulleted-list" | "heading1" | "heading2" | "numbered-list"
type BlockFormat2 = "bulleted-list" | "numbered-list"

// The type of elements we have
type CustomElement = { type: "block-quote", align?: string, children: CustomText[] } |
    { type: "bulleted-list", align?: string, children: CustomText[], depth: number } |
    { type: "heading1", align?: string, children: CustomText[], level: number } |
    { type: "heading2", align?: string, children: CustomText[], level: number } |
    { type: "list-item", align?: string, children: CustomText[], depth: number } |
    { type: "numbered-list", align?: string, children: CustomText[] } |
    { type: "paragraph", align?: string, children: CustomText[] }

// Put all the types for how we use this plugin into its own module
// See https://docs.slatejs.org/concepts/12-typescript
// See https://github.com/ianstormtaylor/slate/issues/3725

declare module 'slate' {
    interface CustomTypes {
        Editor: BaseEditor & ReactEditor & HistoryEditor
        Element: CustomElement
        Text: CustomText
    }
}

const LIST_TYPES = ['numbered-list', 'bulleted-list']
const TEXT_ALIGN_TYPES = ['left', 'center', 'right', 'justify']


/**
 * A function that runs when the user clicks on a block button (a button that changes the format of a block
 * of text rather than whatever is selected). This toggles the format on or off.
 * @param editor - The functions and information required to run the editor.
 * @param format - The name of the format being applied.
 */
// const toggleBlock = (editor: CustomEditor, format: BlockFormat) => {
//
//     const isActive = isBlockActive(editor, format)
//     const isList = format === "numbered-list" || format === "bulleted-list"
//
//     Transforms.unwrapNodes(editor, {
//         match: n => Editor.isBlock(editor, n) && (n.type === "numbered-list" || n.type === "bulleted-list"),
//         split: true
//     })
//
//     Transforms.setNodes(editor, {
//         type: `${isActive ? "paragraph" : isList ? "list-item" : format}`
//     })
//
//     if (!isActive && isList) {
//
//         const block = {type: format, children: []}
//         // @ts-ignore
//         Transforms.wrapNodes(editor, block)
//     }
// }


const toggleBlock = (editor: Editor, format: BlockFormat2, ) => {
  const isActive = isBlockActive(
    editor,
    format,
    TEXT_ALIGN_TYPES.includes(format) ? 'align' : 'type'
  )
  const isList = LIST_TYPES.includes(format)

  Transforms.unwrapNodes(editor, {
    match: n =>
      !Editor.isEditor(n) &&
      SlateElement.isElement(n) &&
      LIST_TYPES.includes(n.type) &&
      !TEXT_ALIGN_TYPES.includes(format),
    split: true,
  })
  let newProperties: Partial<SlateElement>
  if (TEXT_ALIGN_TYPES.includes(format)) {
    newProperties = {
      align: isActive ? undefined : format,
    }
  } else {
    newProperties = {
      type: isActive ? 'paragraph' : isList ? 'list-item' : format,
    }
  }
  Transforms.setNodes<SlateElement>(editor, newProperties)

  if (!isActive && isList) {
    const block = { type: format, children: [], align: undefined}
    //Transforms.wrapNodes(editor, block)
  }
}

/**
 * A function that runs when the user clicks on a non-block button (a button that changes the format of
 * whatever is selected). This toggles the format on or off.
 * @param editor {Editor} The functions and information required to run the editor.
 * @param format - The name of the format being applied.
 */
const toggleMark = (editor: Editor, format: MarkFormat) => {

    const isActive = isMarkActive(editor, format)

    if (isActive) {
        Editor.removeMark(editor, format)
    } else {
        Editor.addMark(editor, format, true)
    }
}

/**
 * A function that runs when the user clicks on the undo button.
 */
const undo = (editor: Editor): void => HistoryEditor.undo(editor)

/**
 * A function that runs when the user clicks on the redo button.
 */
const redo = (editor: Editor): void => HistoryEditor.redo(editor)

/**
 * A function that runs when in toggleBlock, it goes through the nodes in the
 * editor object and works out if the selected block has the specified format.
 * @param editor - The functions and information required to run the editor.
 * @param format - The name of the format being checked.
 */
const isBlockActive = (editor: Editor, format: BlockFormat, blockType : "align" | "type" = "type"): boolean => {

    const {selection} = editor
    if (!selection) return false

    const [match] = Array.from(
        Editor.nodes(editor, {
            at: Editor.unhangRange(editor, selection),
            match: n =>
                !Editor.isEditor(n) &&
                SlateElement.isElement(n) &&
                n[blockType] === format,
        })
    )

    return !!match
}

/**
 * A function that runs when in toggleMark, it goes through the nodes in the
 * editor object and works out if the selected block has the specified format.
 * @param editor {Editor} The functions and information required to run the editor.
 * @param format - The name of the format being checked.
 */
const isMarkActive = (editor: CustomEditor, format: MarkFormat): boolean => {

    const marks = Editor.marks(editor)
    return marks ? marks[format] === true : false
}

/**
 * A component that is used to render the block html elements. It
 * returns the html tag for a given set of blocks.
 * @param attributes - A set of props that are passed to the element.
 * @param children - The children to the element.
 * @param element - The node in the editor.
 * @returns {JSX.Element}
 */
const Element = ({
                     attributes,
                     children,
                     element
                 }: { attributes: { "data-slate-node": "element" }, children: JSX.Element, element: CustomElement }): JSX.Element => {

    switch (element.type) {

        case "block-quote":
            return <blockquote {...attributes} className={"ml-3"}>{children}</blockquote>
        case "bulleted-list":
            return <ul {...attributes} className={"my-3"}>{children}</ul>
        case "heading1":
            return <h3 {...attributes} className={"mt-3 mb-2"}>{children}</h3>
        case "heading2":
            return <h5 {...attributes} className={"mt-3 mb-2"}>{children}</h5>
        case "list-item":
            return <li {...attributes} className={"pl-2 my-1"}>{children}</li>
        case "numbered-list":
            return <ol {...attributes} className={"my-3"}>{children}</ol>
        default:
            return <p {...attributes}>{children}</p>
    }
}

/**
 * A component that is used to render the mark html elements. It
 * returns the html tag for a given set of marks.
 * @param attributes - A set of props that are passed to the element.
 * @param children - The children to the element.
 * @param leaf - The node in the editor.
 * @returns {JSX.Element}
 */
const Leaf = ({
                  attributes,
                  children,
                  leaf
              }: { attributes: { 'data-slate-leaf': true; }, children: JSX.Element, leaf: CustomText }): JSX.Element => {

    // Note that the styles stack
    let newChildren = {...children}

    if (leaf.bold) {
        newChildren = <strong>{newChildren}</strong>
    }

    if (leaf.code) {
        newChildren = <code className={"ml-3"}>{newChildren}</code>
    }

    if (leaf.italic) {
        newChildren = <em>{newChildren}</em>
    }

    if (leaf.underline) {
        newChildren = <u>{newChildren}</u>
    }

    return <span {...attributes}>{newChildren}</span>
}

/**
 * A component that shows a button for a mark format.
 * @param format - The format that the button applies e.g. make text bold.
 * @param icon - The icon to show in the button.
 * @returns {JSX.Element}
 */
const MarkButton = ({format, icon}: { format: MarkFormat, icon: string }): JSX.Element => {

    // Get the current instance of the editor
    const editor: CustomEditor = useSlate()

    return (
        <Button variant={"light"}
                className={"me-1"}
                active={isMarkActive(editor, format)}
                onMouseDown={event => {
                    // The preventDefault stops the focus border
                    event.preventDefault();
                    toggleMark(editor, format)
                }}
        >
            <Icon ariaLabel={format}
                  size={"1.5rem"}
                  icon={icon}
            />

        </Button>
    )
}

/**
 * A component that shows a button for a block format.
 * @param format - The format that the button applies e.g. making a block of text a numbered list.
 * @param icon - The icon to show in the button.
 * @returns {JSX.Element}
 */
const BlockButton = ({format, icon}: { format: BlockFormat2, icon: string }): JSX.Element => {

    // Get the current instance of the editor
    const editor: CustomEditor = useSlate()

    return (
        <Button variant={"light"}
                className={"me-1"}
                active={isBlockActive(editor, format, TEXT_ALIGN_TYPES.includes(format) ? 'align' : 'type')}
                onMouseDown={event => {
                    // The preventDefault stops the focus border
                    event.preventDefault();
                    toggleBlock(editor, format)
                }}
        >
            <Icon ariaLabel={format}
                  icon={icon}
                  size={"1.5rem"}
            />

        </Button>
    )
}

/**
 * A component that shows a button for an undo action.
 * @returns {JSX.Element}
 */
const UndoButton = (): JSX.Element => {

    // Get the current instance of the editor
    const editor: CustomEditor = useSlate()

    return (
        <Button variant={"light"}
                className={"me-1"}
                onMouseDown={event => {
                    // The preventDefault stops the focus border
                    event.preventDefault();
                    undo(editor)
                }}
        >
            <Icon ariaLabel={"Undo"}
                  icon={"bi-arrow-counterclockwise"}
                  size={"1.5rem"}
            />
        </Button>
    )
}

/**
 * A component that shows a button for a redo action.
 * @returns {JSX.Element}
 */
const RedoButton = (): JSX.Element => {

    // Get the current instance of the editor
    const editor: CustomEditor = useSlate()

    return (
        <Button variant={"light"}
                className={"me-1"}
            // The preventDefault stops the focus border
                onMouseDown={event => {
                    event.preventDefault();
                    redo(editor)
                }}
        >
            <Icon ariaLabel={"Redo"}
                  icon={"bi-arrow-clockwise"}
                  size={"1.5rem"}
            />

        </Button>
    )
}

/**
 * A small component that never re-renders for the buttons in editor mode.
 */
const ButtonGroup = memo(() => {

    return (
        <div className={"mb-2 ml-1"}>
            <MarkButton format={"bold"} icon={"bi-type-bold"}/>
            <MarkButton format={"italic"} icon={"bi-type-italic"}/>
            <MarkButton format={"underline"} icon={"bi-type-underline"}/>
            <MarkButton format={"code"} icon={"bi-code-slash"}/>
            {/*<BlockButton format={"heading1"} icon={"bi-type-h1"}/>*/}
            {/*<BlockButton format={"heading2"} icon={"bi-type-h2"}/>*/}
            {/*<BlockButton format={"block-quote"} icon={"bi-quote"}/>*/}
            <BlockButton format={"numbered-list"} icon={"bi-list-ol"}/>
            <BlockButton format={"bulleted-list"} icon={"bi-list-ul"}/>
            <UndoButton/>
            <RedoButton/>
        </div>
    )
});

// Add a display name to memoized components to keep eslint happy
ButtonGroup.displayName = 'ButtonGroup';

/**
 * An interface for the props of the RichTextEditor component.
 */
export interface Props {
    /**
     * The css class to apply to the editor, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The id for the editor that is sent back with the returnEditor function.
     */
    id?: number | string
    /**
     * Whether the editor returnEditor function needs to be dispatched to put it into a store.
     */
    isDispatched: boolean
    /**
     * Whether the editor is in edit mode.
     */
    isEditMode: boolean
    /**
     * A function that runs to return the editor value back to a parent component
     */
    returnEditor: Function
    /**
     * The value for the editor to use when it mounts.
     */
    value: Descendant[]
}

export const RichTextEditor = (props: Props) => {

    const {className, id, isDispatched, isEditMode, returnEditor} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // This is the value for the editor, if passed a previously stored value
    // then that is set as the initial value
    const [value, setValue] = useState<Descendant[]>(props.value)

    // The editor object
    const editor: CustomEditor = useMemo(() => withHistory(withReact(createEditor())), [])

    /**
     * A function that runs when the user clicks away from the editor that sends the
     * value of the editor back to a parent component. Doing this only when the user clicks away
     * is an optimization that prevents a larger re-render as the user is typing.
     */
    const onBlur = () => isDispatched ? dispatch(returnEditor({id, value})) : returnEditor({id, value})

    const finalClassName = `rich-text-editor ${isEditMode ? "p-2 editing border" : ""}`

    return (

        <div className={className}>
            <Slate editor={editor}
                   value={value}
                   onChange={value => setValue(value)}
            >
                {isEditMode &&
                    <ButtonGroup/>
                }

                <div className={finalClassName}>

                    <Editable renderElement={Element}
                              renderLeaf={Leaf}
                              placeholder={"Please add your text"}
                              readOnly={!isEditMode}
                              spellCheck={true}
                              autoFocus={false}
                              onBlur={onBlur}
                    />

                </div>

            </Slate>
        </div>
    )
};

RichTextEditor.propTypes = {

    className: PropTypes.string,
    id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    isEditMode: PropTypes.bool.isRequired,
    isDispatched: PropTypes.bool,
    returnEditor: PropTypes.func.isRequired,
    value: PropTypes.array,
};

RichTextEditor.defaultProps = {

    returnEditor: () => () => {
    },
    isDispatched: true,
    value: [{type: "paragraph", children: [{text: ""}]}],
    isEditMode: false
};