use daedalus_plan::*;

fn three_steps() -> PlanData {
    PlanData {
        extra: Vec::new(),
        steps: vec![
            StepRecord {
                kind: StepKind::Call,
                body: 0,
                covers: Span { start: 0, len: 2 },
                reads: Span { start: 0, len: 0 },
                writes: Span { start: 0, len: 1 },
            },
            StepRecord {
                kind: StepKind::Transfer,
                body: 1,
                covers: Span { start: 0, len: 0 },
                reads: Span { start: 0, len: 1 },
                writes: Span { start: 1, len: 1 },
            },
            StepRecord {
                kind: StepKind::Wait,
                body: 2,
                covers: Span { start: 2, len: 1 },
                reads: Span { start: 1, len: 1 },
                writes: Span { start: 2, len: 0 },
            },
        ],
        nodes: vec![7, 8, 9],
        slots: vec![0, 1],
        spaces: vec![0, 1],
        types: vec![3],
        bodies: vec![10, 11, 12],
    }
}

fn bytes_of(plan: &PlanData) -> Vec<u8> {
    let mut out = Vec::new();
    encode(plan, &mut out).unwrap();
    out
}

fn unhex(text: &str) -> Vec<u8> {
    let digits: Vec<u8> = text
        .bytes()
        .filter(|b| !b.is_ascii_whitespace())
        .map(|b| (b as char).to_digit(16).unwrap() as u8)
        .collect();
    digits
        .chunks(2)
        .map(|pair| pair[0] << 4 | pair[1])
        .collect()
}

#[test]
fn round_trip_yields_equal_records() {
    let plan = three_steps();
    let bytes = bytes_of(&plan);
    let view = PlanView::parse(&bytes).unwrap();
    assert_eq!(view.step_count(), 3);
    assert_eq!(view.steps().collect::<Vec<_>>(), plan.steps);
    let covers = plan.steps[0].covers;
    assert_eq!(view.node_list(covers).unwrap().collect::<Vec<_>>(), [7, 8]);
    assert_eq!(
        view.slot_list(plan.steps[1].writes)
            .unwrap()
            .collect::<Vec<_>>(),
        [1]
    );
    assert_eq!(view.spaces().collect::<Vec<_>>(), plan.spaces);
    assert_eq!(view.types().collect::<Vec<_>>(), plan.types);
    assert_eq!(view.bodies().collect::<Vec<_>>(), plan.bodies);
}

#[test]
fn golden_bytes_for_a_three_step_plan() {
    let golden = unhex(include_str!("golden/three_steps.hex"));
    assert_eq!(bytes_of(&three_steps()), golden);
    assert_eq!(PlanView::parse(&golden).unwrap().hash(), GOLDEN_HASH);
}

const GOLDEN_HASH: u64 = u64::from_le_bytes(GOLDEN_HASH_BYTES);
const GOLDEN_HASH_BYTES: [u8; 8] = [142, 0, 142, 44, 217, 170, 102, 213];

#[test]
fn truncated_input_is_refused() {
    let bytes = bytes_of(&three_steps());
    assert_eq!(PlanView::parse(&[]).unwrap_err(), PlanError::Truncated);
    assert_eq!(
        PlanView::parse(&bytes[..10]).unwrap_err(),
        PlanError::Truncated
    );
    assert!(matches!(
        PlanView::parse(&bytes[..bytes.len() - 1]),
        Err(PlanError::Bounds { .. })
    ));
}

#[test]
fn bad_magic_and_format_are_refused() {
    let mut bytes = bytes_of(&three_steps());
    bytes[0] = b'X';
    assert_eq!(PlanView::parse(&bytes).unwrap_err(), PlanError::BadMagic);
    let mut bytes = bytes_of(&three_steps());
    bytes[8] = 9;
    assert_eq!(
        PlanView::parse(&bytes).unwrap_err(),
        PlanError::UnknownFormat(9)
    );
}

#[test]
fn bad_records_are_refused() {
    let mut plan = three_steps();
    plan.steps[0].covers = Span { start: 2, len: 2 };
    assert_eq!(
        PlanView::parse(&bytes_of(&plan)).unwrap_err(),
        PlanError::Bounds {
            section: "nodes",
            at: 2
        }
    );
    let mut bytes = bytes_of(&three_steps());
    let steps_at = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
    bytes[steps_at] = 200;
    assert_eq!(
        PlanView::parse(&bytes).unwrap_err(),
        PlanError::UnknownKind(200)
    );
}

#[test]
fn a_huge_span_is_a_bounds_error_and_not_an_overflow() {
    let mut plan = three_steps();
    plan.steps[1].reads = Span {
        start: u32::MAX,
        len: u32::MAX,
    };
    assert!(matches!(
        PlanView::parse(&bytes_of(&plan)),
        Err(PlanError::Bounds {
            section: "slots",
            ..
        })
    ));
}

#[test]
fn parse_never_panics_on_noise() {
    let valid = bytes_of(&three_steps());
    let mut state = 0x1234_5678u32;
    let mut next = move || {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        (state >> 16) as u8
    };
    for round in 0..10_000 {
        let mut bytes = if round % 2 == 0 {
            valid.clone()
        } else {
            let len = usize::from(next()) * 2;
            (0..len).map(|_| next()).collect()
        };
        for _ in 0..4 {
            if !bytes.is_empty() {
                let at = (usize::from(next()) << 8 | usize::from(next())) % bytes.len();
                bytes[at] = next();
            }
        }
        if let Ok(view) = PlanView::parse(&bytes) {
            for step in view.steps() {
                let _ = view.node_list(step.covers).map(Iterator::count);
                let _ = view.slot_list(step.reads).map(Iterator::count);
            }
            let _ = (
                view.spaces().count(),
                view.types().count(),
                view.bodies().count(),
            );
        }
    }
}
