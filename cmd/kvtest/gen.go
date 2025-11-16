package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sudeep9/sqlitekv"
)

func createOrgs(ctx context.Context, st *State, orgCount int) (err error) {
	for i := 0; i < orgCount; i++ {
		org := &Org{
			BaseCollection: BaseCollection{
				Meta: Metadata{
					Its: time.Now().Unix(),
					Cas: st.Cas(),
				},
			},
			Name: fmt.Sprintf("Org %d", i),
		}

		_, err = st.orgCol.Insert(ctx, org)
		if err != nil {
			return err
		}

		err = createPatients(ctx, st, org.Id)
		if err != nil {
			fmt.Printf("failed to create patients for org %d: %v\n", org.Id, err)
		}

		fmt.Printf("Created org: %d\n", org.Id)
	}

	return
}

func createPatients(ctx context.Context, st *State, oid int64) (err error) {
	for i := 0; i < 1000; i++ {
		patient := &Patient{
			BaseCollection: BaseCollection{
				Meta: Metadata{
					Its: time.Now().Unix(),
					Cas: st.Cas(),
				},
			},
			Oid:   oid,
			Name:  fmt.Sprintf("Patient %d-%d", oid, i),
			Phone: fmt.Sprintf("%d", 12345678+i),
			Age:   20 + i,
		}
		_, err = st.patientCol.Insert(ctx, patient)
		if err != nil {
			return err
		}

		err = createRx(ctx, st, patient.Id)
		if err != nil {
			fmt.Printf("failed to create rx for patient %d: %v\n", patient.Id, err)
			return err
		}
	}

	return
}

func createRx(ctx context.Context, st *State, pid int64) (err error) {
	for i := 0; i < 10; i++ {
		rxid, err := st.counter.Next(ctx)
		if err != nil {
			return err
		}
		rx := &Rx{
			BaseCollection: BaseCollection{
				Meta: Metadata{
					Its: time.Now().Unix(),
					Cas: st.Cas(),
				},
			},
			Pid:        pid,
			Medication: fmt.Sprintf("Medication %d", rxid),
		}

		done := false
		for !done {
			_, err = st.rxCol.Insert(ctx, rx)
			if err != nil {
				if err == sqlitekv.ErrPrimaryConstraint {
					err = nil
					continue
				}
				return err
			}

			done = true
		}
	}
	return
}
