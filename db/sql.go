package db

const(
	SQL_KDJ_FEAT_DAT=`
		SELECT
			f.fid, f.smp_num, f.fd_num, f.weight, k.seq, k.k, k.d, k.j
		FROM
			kdj_feat_dat k
				INNER JOIN
			(SELECT
				*
			FROM
				indc_feat
			WHERE
				indc = 'KDJ' AND cytp = ?
					AND bysl = ?
					AND smp_num = ?) f USING (fid)
		ORDER BY k.fid, f.fd_num desc, k.seq
	`
)