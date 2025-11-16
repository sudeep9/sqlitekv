package main

/*
func createOrgs(ctx context.Context, st *State, orgCount int) (err error) {
	for i := 0; i < orgCount; i++ {
		orgId, err := st.counter.Next(ctx)
		if err != nil {
			return err
		}

		org := &Org{
			Meta: Metadata{
				Key: fmt.Sprintf("/o/%d", orgId),
				Its: time.Now().Unix(),
				Cas: st.Cas(),
			},
			Name: fmt.Sprintf("Org %d", orgId),
		}

		buf, err := json.Marshal(org)
		if err != nil {
			return err
		}

		org.Meta.RowId, err = st.orgCol.Put(ctx, org.Meta.Key, buf)
		if err != nil {
			return err
		}

		err = st.kv.WithTx(func() error {
			err = createPatients(ctx, st, org.Meta.Key)
			if err != nil {
				fmt.Printf("failed to create patients for org %s: %v\n", org.Meta.Key, err)
			}
			return nil
		})
		if err != nil {
			return err
		}

		fmt.Printf("Created org: %s\n", org.Meta.Key)
	}

	return
}

func createPatients(ctx context.Context, st *State, orgKey string) (err error) {
	for i := 0; i < 1000; i++ {
		patientId, err := st.counter.Next(ctx)
		if err != nil {
			return err
		}
		patient := &Patient{
			Meta: Metadata{
				Key: fmt.Sprintf("%s/p/%d", orgKey, patientId),
				Its: time.Now().Unix(),
				Cas: st.Cas(),
			},
			Name:  fmt.Sprintf("Patient %d", patientId),
			Phone: fmt.Sprintf("%d", 12345678+patientId),
			Age:   20 + i,
		}
		buf, err := json.Marshal(patient)
		if err != nil {
			return err
		}

		patient.Meta.RowId, err = st.patientCol.Put(ctx, patient.Meta.Key, buf)
		if err != nil {
			return err
		}

		err = createRx(ctx, st, patient.Meta.Key)
		if err != nil {
			fmt.Printf("failed to create rx for patient %s: %v\n", patient.Meta.Key, err)
			return err
		}
	}

	return
}

func createRx(ctx context.Context, st *State, patientKey string) (err error) {
	for i := 0; i < 10; i++ {
		rxid, err := st.counter.Next(ctx)
		if err != nil {
			return err
		}
		rx := &Rx{
			Meta: Metadata{
				Key: fmt.Sprintf("%s/rx/%d", patientKey, rxid),
				Its: time.Now().Unix(),
				Cas: st.Cas(),
			},
			Medication: fmt.Sprintf("Medication %d", rxid),
		}
		buf, err := json.Marshal(rx)
		if err != nil {
			return err
		}
		rx.Meta.RowId, err = st.rxCol.Put(ctx, rx.Meta.Key, buf)
		if err != nil {
			return err
		}
	}
	return
}
*/
