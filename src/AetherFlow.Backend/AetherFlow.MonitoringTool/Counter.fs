namespace AetherFlow.MonitoringTool

module Counter =
    open Elmish
    open Avalonia.FuncUI
    open Avalonia.FuncUI.DSL
    open Avalonia.Controls
    open Avalonia.Layout

    // Model: der Zustand der App
    type State = { count: int }
    let init = { count = 0 }

    // Msg: alle möglichen Ereignisse
    type Msg =
        | Increment
        | Decrement
        | Reset

    // Update: berechnet aus Ereignis + altem Zustand den neuen Zustand
    let update (msg: Msg) (state: State) : State =
        match msg with
        | Increment -> { state with count = state.count + 1 }
        | Decrement -> { state with count = state.count - 1 }
        | Reset     -> { count = 0 }

    // View: erzeugt das UI aus dem Zustand
    let view (state: State) (dispatch) =
        StackPanel.create [
            StackPanel.horizontalAlignment HorizontalAlignment.Center
            StackPanel.verticalAlignment VerticalAlignment.Center
            StackPanel.spacing 16.0
            StackPanel.children [
                TextBlock.create [
                    TextBlock.fontSize 48.0
                    TextBlock.horizontalAlignment HorizontalAlignment.Center
                    TextBlock.text (string state.count)
                ]
                StackPanel.create [
                    StackPanel.orientation Orientation.Horizontal
                    StackPanel.horizontalAlignment HorizontalAlignment.Center
                    StackPanel.spacing 8.0
                    StackPanel.children [
                        Button.create [
                            Button.content "−"
                            Button.onClick (fun _ -> dispatch Decrement)
                        ]
                        Button.create [
                            Button.content "Reset"
                            Button.onClick (fun _ -> dispatch Reset)
                        ]
                        Button.create [
                            Button.content "+"
                            Button.onClick (fun _ -> dispatch Increment)
                        ]
                    ]
                ]
            ]
        ]